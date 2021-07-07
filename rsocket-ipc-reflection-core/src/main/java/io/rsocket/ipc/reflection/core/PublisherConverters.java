package io.rsocket.ipc.reflection.core;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.reflections8.ReflectionUtils;

public class PublisherConverters {

	private static final Map<Optional<Void>, List<PublisherConverter<?>>> DEFAULT_CONVERTER_METHODS = new ConcurrentHashMap<>(
			1);

	public static Optional<PublisherConverter<?>> lookup(Class<?> classType) {
		if (classType == null)
			return Optional.empty();
		Stream<PublisherConverter<?>> publisherConverterStream = DEFAULT_CONVERTER_METHODS
				.computeIfAbsent(Optional.empty(), nil -> loadDefaultConverters()).stream();
		Optional<PublisherConverter<?>> op = publisherConverterStream.filter(v -> v.appliesTo(classType)).findFirst()
				.map(v -> v);
		return op;
	}

	@SuppressWarnings("unchecked")
	private static List<PublisherConverter<?>> loadDefaultConverters() {
		Set<Method> methods = ReflectionUtils.getAllMethods(PublisherConverter.class,
				m -> Modifier.isStatic(m.getModifiers()), m -> Modifier.isPublic(m.getModifiers()),
				m -> m.getParameterCount() == 0, m -> PublisherConverter.class.isAssignableFrom(m.getReturnType()));
		Stream<PublisherConverter<?>> stream = methods.stream().map(v -> {
			try {
				return v.invoke(null);
			} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
				throw java.lang.RuntimeException.class.isAssignableFrom(e.getClass())
						? java.lang.RuntimeException.class.cast(e)
						: new java.lang.RuntimeException(e);
			}
		}).map(v -> (PublisherConverter<?>) v);
		stream = stream.sorted(Comparator.comparing(v -> v.priority()));
		return Collections.unmodifiableList(stream.collect(Collectors.toList()));

	}

}
