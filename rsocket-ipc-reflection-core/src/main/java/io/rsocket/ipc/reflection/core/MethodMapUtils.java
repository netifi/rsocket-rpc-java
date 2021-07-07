package io.rsocket.ipc.reflection.core;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.logging.Logger;
import java.util.stream.Stream;

import org.reflections8.ReflectionUtils;

import io.rsocket.ipc.util.IPCUtils;
import javassist.Modifier;
import reactor.core.publisher.Mono;

public class MethodMapUtils {
	private static final Class<?> THIS_CLASS = new Object() {
	}.getClass().getEnclosingClass();
	private static final Logger logger = java.util.logging.Logger.getLogger(THIS_CLASS.getName());

	private static final Map<Class<?>, Class<?>> PRIMITIVES_TO_WRAPPERS = getPrimitivesToWrappers();

	private static Map<Class<?>, Class<?>> getPrimitivesToWrappers() {
		Map<Class<?>, Class<?>> map = new HashMap<>();
		map.put(boolean.class, Boolean.class);
		map.put(byte.class, Byte.class);
		map.put(char.class, Character.class);
		map.put(double.class, Double.class);
		map.put(float.class, Float.class);
		map.put(int.class, Integer.class);
		map.put(long.class, Long.class);
		map.put(short.class, Short.class);
		map.put(void.class, Void.class);
		return Collections.unmodifiableMap(map);
	}

	private static final Map<Class<?>, Class<?>> WRAPPERS_TO_PRIMITIVES = getWrappersToPrimitives();

	private static Map<Class<?>, Class<?>> getWrappersToPrimitives() {
		Map<Class<?>, Class<?>> map = new HashMap<>();
		PRIMITIVES_TO_WRAPPERS.entrySet().forEach(e -> map.put(e.getValue(), e.getKey()));
		return Collections.unmodifiableMap(map);
	}

	@SuppressWarnings("unchecked")
	public static Map<String, Method> getMappedMethods(Class<?> classType, boolean includeVariations) {
		Objects.requireNonNull(classType);
		Map<String, List<Method>> lookup = new LinkedHashMap<>();
		{
			Set<Method> methods = ReflectionUtils.getAllMethods(classType, m -> Modifier.isPublic(m.getModifiers()),
					m -> !Modifier.isStatic(m.getModifiers()));
			methods.stream().flatMap(
					m -> getMethodSignatures(m, includeVariations).stream().map(v -> new SimpleImmutableEntry<>(v, m)))
					.forEach(ent -> {
						Method candidateMethod = ent.getValue();
						List<Method> methodList = lookup.computeIfAbsent(ent.getKey(), nil -> new ArrayList<>());
						for (int i = 0; i < methodList.size(); i++) {
							Method currentMethod = methodList.get(i);
							if (isOverridingMethod(currentMethod, candidateMethod)) {
								methodList.set(i, candidateMethod);
								return;
							}
							if (isOverridingMethod(candidateMethod, currentMethod))
								return;
						}
						methodList.add(candidateMethod);
					});
		}
		Map<String, Method> result = new LinkedHashMap<>();
		for (Entry<String, List<Method>> ent : lookup.entrySet()) {
			String key = ent.getKey();
			List<Method> methods = ent.getValue();
			if (methods.size() > 1) {
				logger.warning(String.format("multiple methods match signature, skipping:%s", key));
				continue;
			}
			result.put(key, methods.get(0));
		}
		return result;
	}

	public static boolean compatibleMethods(Method method1, Method method2) {
		return isOverridingMethod(method1, method2) || isOverridingMethod(method2, method1);
	}

	private static boolean isOverridingMethod(Method parentCandidate, Method childCandidate) {
		if (Objects.equals(parentCandidate, childCandidate))
			return true;
		if (!parentCandidate.getName().equals(childCandidate.getName()))
			return false;
		if (parentCandidate.getParameterCount() != childCandidate.getParameterCount())
			return false;
		if (!parentCandidate.getDeclaringClass().isAssignableFrom(childCandidate.getDeclaringClass()))
			return false;
		Function<Method, Integer> restrictionLevel = m -> {
			int modifiers = m.getModifiers();
			if (Modifier.isPrivate(modifiers))
				return 0;
			if (Modifier.isProtected(modifiers))
				return 2;
			if (Modifier.isPublic(modifiers))
				return 3;
			return 1;// package level
		};
		if (restrictionLevel.apply(childCandidate) < restrictionLevel.apply(parentCandidate))
			return false;
		Class<?>[] parentPts = parentCandidate.getParameterTypes();
		Class<?>[] childPts = childCandidate.getParameterTypes();
		for (int i = 0; i < parentPts.length; i++) {
			Class<?> parentPt = parentPts[i];
			Class<?> childPt = childPts[i];
			if (!parentPt.isAssignableFrom(childPt))
				return false;
		}
		return true;
	}

	private static Set<String> getMethodSignatures(Method method, boolean includeVariations) {
		Set<String> methodSignatures = new LinkedHashSet<>();
		if (includeVariations)
			methodSignatures.add(method.getName());
		Class<?>[] parameterTypes = method.getParameterTypes();
		List<Boolean> options = includeVariations ? Arrays.asList(false, true) : Arrays.asList(false);
		for (boolean lowercase : options) {
			for (boolean simpleName : options) {
				for (boolean disableParameterTypes : options) {
					for (boolean unwrapPrimitives : options) {
						Stream<Entry<String, String>> stream = Stream.empty();
						stream = Stream.concat(stream, Stream.of(new SimpleImmutableEntry<>("n", method.getName())));
						if (!disableParameterTypes) {
							for (Class<?> pt : parameterTypes) {
								if (unwrapPrimitives) {
									Class<?> primType = WRAPPERS_TO_PRIMITIVES.get(pt);
									pt = primType != null ? primType : pt;
								}
								stream = Stream.concat(stream, Stream.of(new SimpleImmutableEntry<>("at",
										simpleName ? pt.getSimpleName() : pt.getName())));
							}
						}
						if (lowercase)
							stream = stream.map(e -> new SimpleImmutableEntry<>(e.getKey().toLowerCase(),
									e.getValue().toLowerCase()));
						methodSignatures.add(IPCUtils.encodeEntries(stream));
					}
				}
			}
		}
		return methodSignatures;
	}

	public static Optional<Type> getRequestChannelParameterType(Method method) {
		if (method == null)
			return Optional.empty();
		if (method.getParameterCount() != 1)
			return Optional.empty();
		Optional<PublisherConverter<?>> payloadConverter = PublisherConverters.lookup(method.getParameterTypes()[0]);
		if (!payloadConverter.isPresent())
			return Optional.empty();
		if (!PublisherConverters.lookup(method.getReturnType()).isPresent())
			return Optional.empty();
		Type type = method.getGenericParameterTypes()[0];
		Optional<Type> typeArgument = payloadConverter.get().getPublisherTypeArgument(type);
		if (!typeArgument.isPresent())
			return Optional.empty();
		return Optional.of(typeArgument.get());
	}

	public static boolean isFireAndForget(Method method) {
		if (method == null)
			return false;
		if (method.getReturnType().equals(Void.TYPE))
			return true;
		if (!Mono.class.isAssignableFrom(method.getReturnType()))
			return false;
		Type typeArgument = getPublisherTypeArgument(Mono.class, method.getGenericReturnType()).orElse(null);
		if (typeArgument == null)
			return false;
		if (typeArgument.equals(Void.TYPE))
			return true;
		if (typeArgument.equals(Void.class))
			return true;
		if (typeArgument.equals(void.class))
			return true;
		return false;
	}

	public static Optional<Type> getPublisherTypeArgument(Class<?> superClassLimit, Type type) {
		if (type == null)
			return Optional.empty();
		if (type instanceof ParameterizedType) {
			ParameterizedType pt = (ParameterizedType) type;
			Type[] actualTypeArguments = pt.getActualTypeArguments();
			if (actualTypeArguments != null && actualTypeArguments.length == 1)
				return Optional.of(actualTypeArguments[0]);
		}
		if (!(type instanceof Class))
			return Optional.empty();
		Class<?> superClass = ((Class<?>) type).getSuperclass();
		if (superClass != null && superClassLimit != null && !superClassLimit.isAssignableFrom(superClass))
			return Optional.empty();
		return getPublisherTypeArgument(superClassLimit, superClass);
	}

}
