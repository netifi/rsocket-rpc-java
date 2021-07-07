package io.rsocket.ipc.marshallers;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.util.Objects;
import java.util.function.Function;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.reflect.TypeToken;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.rsocket.ipc.Unmarshaller;

public class GsonUnmarshaller<X> implements Unmarshaller<X> {

	public static <X> GsonUnmarshaller<X> create(Gson gson, TypeToken<X> typeToken) {
		return create(gson, typeToken == null ? null : typeToken.getType());
	}

	public static <X> GsonUnmarshaller<X> create(Gson gson, Class<X> classType) {
		return create(gson, (Type) classType);
	}

	public static <X> GsonUnmarshaller<X> create(Gson gson, Type type) {
		Objects.requireNonNull(gson);
		Objects.requireNonNull(type);
		return new GsonUnmarshaller<>(isSupplier -> parseUnchecked(gson, type, isSupplier));
	}

	public static GsonUnmarshaller<Object[]> create(Gson gson, Type[] types) {
		Objects.requireNonNull(gson);
		Objects.requireNonNull(types);
		for (Type type : types)
			Objects.requireNonNull(type);
		Function<ByteBuf, Object[]> parser = bb -> {
			JsonArray jarr = parseUnchecked(gson, JsonArray.class, bb);
			Object[] result = new Object[jarr.size()];
			for (int i = 0; i < jarr.size(); i++) {
				Type type = types == null || types.length <= i ? null : types[i];
				if (type == null)
					type = Object.class;
				result[i] = gson.fromJson(jarr.get(i), type);
			}
			return result;
		};
		return new GsonUnmarshaller<>(parser);
	}

	private final Function<ByteBuf, X> parser;

	protected GsonUnmarshaller(Function<ByteBuf, X> parser) {
		this.parser = Objects.requireNonNull(parser);
	}

	@Override
	public X apply(ByteBuf byteBuf) {
		return parser.apply(byteBuf);
	}

	private static <Y> Y parseUnchecked(Gson gson, Type type, ByteBuf byteBuf) {
		Objects.requireNonNull(gson);
		Objects.requireNonNull(byteBuf);
		type = type != null ? type : Object.class;
		try (InputStream is = new ByteBufInputStream(byteBuf); InputStreamReader reader = new InputStreamReader(is);) {
			return gson.fromJson(reader, type);
		} catch (IOException e) {
			throw java.lang.RuntimeException.class.isAssignableFrom(e.getClass())
					? java.lang.RuntimeException.class.cast(e)
					: new java.lang.RuntimeException(e);
		}
	}

}
