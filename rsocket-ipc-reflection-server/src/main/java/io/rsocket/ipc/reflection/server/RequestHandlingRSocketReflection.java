package io.rsocket.ipc.reflection.server;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.reactivestreams.Publisher;

import io.netty.buffer.ByteBuf;
import io.opentracing.Tracer;
import io.rsocket.ipc.IPCRSocket;
import io.rsocket.ipc.Marshaller;
import io.rsocket.ipc.MetadataDecoder;
import io.rsocket.ipc.RequestHandlingRSocket;
import io.rsocket.ipc.Server;
import io.rsocket.ipc.Server.U;
import io.rsocket.ipc.Unmarshaller;
import io.rsocket.ipc.reflection.core.MethodMapUtils;
import io.rsocket.ipc.reflection.core.PublisherConverter;
import io.rsocket.ipc.reflection.core.PublisherConverters;
import io.rsocket.ipc.util.IPCUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

public class RequestHandlingRSocketReflection extends RequestHandlingRSocket {
	private static final Class<?> THIS_CLASS = new Object() {
	}.getClass().getEnclosingClass();
	private static final Logger logger = java.util.logging.Logger.getLogger(THIS_CLASS.getName());
	private Scheduler subscribeOnScheduler;

	public RequestHandlingRSocketReflection(Scheduler subscribeOnScheduler) {
		super();
		this.subscribeOnScheduler = subscribeOnScheduler;
	}

	public RequestHandlingRSocketReflection(Scheduler subscribeOnScheduler, MetadataDecoder decoder) {
		super(decoder);
		this.subscribeOnScheduler = subscribeOnScheduler;
	}

	public RequestHandlingRSocketReflection(Scheduler subscribeOnScheduler, Tracer tracer) {
		super(tracer);
		this.subscribeOnScheduler = subscribeOnScheduler;
	}

	public <S> void register(Class<S> serviceType, S service, Marshaller<Object> resultMarshaller,
			Unmarshaller<Object[]> unmarshaller) {
		register(serviceType, service, resultMarshaller, (paramTypes, bb) -> unmarshaller.apply(bb));
	}

	public <S> void register(Class<S> serviceType, S service, Marshaller<Object> resultMarshaller,
			BiFunction<Type[], ByteBuf, Object[]> argumentUnmarshaller) {
		Objects.requireNonNull(serviceType);
		Objects.requireNonNull(service);
		Objects.requireNonNull(resultMarshaller);
		Objects.requireNonNull(argumentUnmarshaller);
		Map<String, Method> methodMapping = MethodMapUtils.getMappedMethods(serviceType, true);
		Set<String> serviceNameTracker = new HashSet<>();
		for (boolean lowercase : Arrays.asList(false, true)) {
			for (boolean simpleName : Arrays.asList(false, true)) {
				String serviceName = simpleName ? serviceType.getSimpleName() : serviceType.getName();
				serviceName = lowercase ? serviceName.toLowerCase() : serviceName;
				if (!serviceNameTracker.add(serviceName))
					continue;
				for (Entry<String, Method> ent : methodMapping.entrySet()) {
					String route = ent.getKey();
					Method method = ent.getValue();
					logger.log(Level.INFO,
							String.format("registering request handler. route:%s.%s", serviceName, route));
					register(service, argumentUnmarshaller, createServiceBuilder(serviceName, resultMarshaller), route,
							method);
				}
			}
		}
	}

	private <S> void register(S service, BiFunction<Type[], ByteBuf, Object[]> argumentUnmarshaller,
			U<Object> serviceBuilder, String route, Method method) {
		if (registerRequestChannel(service, argumentUnmarshaller, serviceBuilder, route, method))
			return;
		if (registerFireAndForget(service, argumentUnmarshaller, serviceBuilder, route, method))
			return;
		Optional<PublisherConverter<?>> returnPublisherConverter = PublisherConverters.lookup(method.getReturnType());
		if (registerRequestStream(service, argumentUnmarshaller, serviceBuilder, route, method,
				returnPublisherConverter))
			return;
		if (registerRequestResponse(service, argumentUnmarshaller, serviceBuilder, route, method,
				returnPublisherConverter))
			return;
		String errorMessage = String.format("unable to map method. serviceInstanceType:%s methodRoute:%s method:%s",
				service.getClass().getName(), route, method);
		throw new IllegalArgumentException(errorMessage);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private <S> boolean registerRequestChannel(S service, BiFunction<Type[], ByteBuf, Object[]> argumentUnmarshaller,
			U<Object> serviceBuilder, String route, Method method) {
		Optional<Type> requestChannelParameterType = MethodMapUtils.getRequestChannelParameterType(method);
		if (!requestChannelParameterType.isPresent())
			return false;
		PublisherConverter returnPublisherConverter = PublisherConverters.lookup(method.getReturnType()).get();
		Type[] typeArguments = new Type[] { requestChannelParameterType.get() };
		IPCRSocket ipcrSocket = serviceBuilder.unmarshall(createUnmarshaller(typeArguments, argumentUnmarshaller))
				.requestChannel(route, (first, publisher, md) -> {
					Flux<Object> argFlux = Flux.from(publisher).map(args -> args[0]);
					md.retain();
					Runnable mdRelease = () -> {
						if (md.refCnt() > 0)
							md.release();
					};
					return IPCUtils.onError(() -> {
						return asFlux(() -> {
							Object result = invoke(service, method, new Object[] { argFlux });
							return returnPublisherConverter.toPublisher(result);
						}, this.subscribeOnScheduler).doOnTerminate(mdRelease);
					}, mdRelease);
				}).toIPCRSocket();
		this.withEndpoint(ipcrSocket);
		return true;
	}

	private <S> boolean registerFireAndForget(S service, BiFunction<Type[], ByteBuf, Object[]> argumentUnmarshaller,
			U<Object> serviceBuilder, String route, Method method) {
		if (!MethodMapUtils.isFireAndForget(method))
			return false;
		IPCRSocket ipcrSocket = serviceBuilder
				.unmarshall(createUnmarshaller(method.getGenericParameterTypes(), argumentUnmarshaller))
				.fireAndForget(route, (data, md) -> {
					Mono<Mono<Void>> wrappedMono = asMono(() -> {
						invoke(service, method, data);
						return Mono.empty();
					}, this.subscribeOnScheduler);
					return wrappedMono.flatMap(v -> v);
				}).toIPCRSocket();
		this.withEndpoint(ipcrSocket);
		return true;

	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private <S> boolean registerRequestStream(S service, BiFunction<Type[], ByteBuf, Object[]> argumentUnmarshaller,
			U<Object> serviceBuilder, String route, Method method,
			Optional<PublisherConverter<?>> returnPublisherConverter) {
		if (!returnPublisherConverter.isPresent())
			return false;
		if (Mono.class.isAssignableFrom(method.getReturnType()))
			return false;
		PublisherConverter publisherConverter = returnPublisherConverter.get();
		IPCRSocket ipcrSocket = serviceBuilder
				.unmarshall(createUnmarshaller(method.getGenericParameterTypes(), argumentUnmarshaller))
				.requestStream(route, (data, md) -> {
					return asFlux(() -> {
						Object result = invoke(service, method, data);
						return publisherConverter.toPublisher(result);
					}, this.subscribeOnScheduler);
				}).toIPCRSocket();
		this.withEndpoint(ipcrSocket);
		return true;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private <S> boolean registerRequestResponse(S service, BiFunction<Type[], ByteBuf, Object[]> argumentUnmarshaller,
			U<Object> serviceBuilder, String route, Method method,
			Optional<PublisherConverter<?>> returnPublisherConverter) {
		PublisherConverter publisherConverter = returnPublisherConverter.orElse(null);
		IPCRSocket ipcrSocket = serviceBuilder
				.unmarshall(createUnmarshaller(method.getGenericParameterTypes(), argumentUnmarshaller))
				.requestResponse(route, (data, md) -> {
					Mono<Mono<Object>> wrappedMono = asMono(() -> {
						Object result = invoke(service, method, data);
						if (publisherConverter != null)
							return Mono.from(publisherConverter.toPublisher(result));
						return Mono.just(result);
					}, this.subscribeOnScheduler);
					return wrappedMono.flatMap(v -> v);
				}).toIPCRSocket();
		this.withEndpoint(ipcrSocket);
		return true;
	}

	public Scheduler getSubscribeOnScheduler() {
		return subscribeOnScheduler;
	}

	public void setSubscribeOnScheduler(Scheduler subscribeOnScheduler) {
		this.subscribeOnScheduler = subscribeOnScheduler;
	}

	private static <X> U<X> createServiceBuilder(String serviceName, Marshaller<X> resultMarshaller) {
		return Server.service(serviceName).noMeterRegistry().noTracer().marshall(resultMarshaller);
	}

	private static Unmarshaller<Object[]> createUnmarshaller(Type[] typeArguments,
			BiFunction<Type[], ByteBuf, Object[]> argumentDeserializer) {
		return bb -> argumentDeserializer.apply(typeArguments, bb);
	}

	private static <X> Flux<X> asFlux(Supplier<Publisher<X>> supplier, Scheduler scheduler) {
		Supplier<Publisher<X>> memoizedSupplier = memoized(supplier);
		Flux<X> result = Flux.defer(memoizedSupplier);
		if (scheduler != null)
			result = result.subscribeOn(scheduler);
		return result;
	}

	private static <X> Mono<X> asMono(Supplier<X> supplier, Scheduler scheduler) {
		Supplier<X> memoizedSupplier = memoized(supplier);
		Mono<X> result = Mono.fromSupplier(memoizedSupplier);
		if (scheduler != null)
			result = result.subscribeOn(scheduler);
		return result;
	}

	private static <S> Object invoke(S serivce, Method method, Object[] arguments) {
		try {
			return method.invoke(serivce, arguments);
		} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
			throw java.lang.RuntimeException.class.isAssignableFrom(e.getClass())
					? java.lang.RuntimeException.class.cast(e)
					: new java.lang.RuntimeException(e);
		}
	}

	private static <X> Supplier<X> memoized(Supplier<X> supplier) {
		Objects.requireNonNull(supplier);
		AtomicReference<Optional<X>> ref = new AtomicReference<>();
		return () -> {
			if (ref.get() == null)
				synchronized (ref) {
					if (ref.get() == null)
						ref.set(Optional.ofNullable(supplier.get()));
				}
			return ref.get().orElse(null);
		};
	}

}
