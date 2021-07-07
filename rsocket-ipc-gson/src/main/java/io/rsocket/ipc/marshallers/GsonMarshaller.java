package io.rsocket.ipc.marshallers;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Objects;

import com.google.gson.Gson;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufOutputStream;
import io.rsocket.ipc.Marshaller;

public class GsonMarshaller<X> implements Marshaller<X> {

	public static <X> GsonMarshaller<X> create(Gson gson) {
		return create(gson, ByteBufAllocator.DEFAULT);
	}

	public static <X> GsonMarshaller<X> create(Gson gson, ByteBufAllocator allocator) {
		return new GsonMarshaller<>(gson, allocator);
	}

	private Gson gson;
	private ByteBufAllocator allocator;

	protected GsonMarshaller(Gson gson, ByteBufAllocator allocator) {
		this.gson = Objects.requireNonNull(gson);
		this.allocator = Objects.requireNonNull(allocator);
	}

	@Override
	public ByteBuf apply(X object) {
		ByteBuf buffer = allocator.buffer();
		try (ByteBufOutputStream os = new ByteBufOutputStream(buffer);
				OutputStreamWriter writer = new OutputStreamWriter(os);) {
			gson.toJson(object, writer);
			writer.flush();
		} catch (IOException e) {
			throw java.lang.RuntimeException.class.isAssignableFrom(e.getClass())
					? java.lang.RuntimeException.class.cast(e)
					: new java.lang.RuntimeException(e);
		}
		return buffer;
	}

	public Gson getGson() {
		return gson;
	}

}
