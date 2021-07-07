package io.rsocket.ipc.decoders;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.PrimitiveIterator.OfInt;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.rsocket.ipc.mimetype.MimeType;
import io.rsocket.ipc.util.IPCUtils;
import io.rsocket.metadata.CompositeMetadataCodec;
import io.rsocket.metadata.WellKnownMimeType;

public class MetadataWriter {

	private final ByteBufAllocator allocator;
	private CompositeByteBuf _compositeByteBuf;

	public MetadataWriter() {
		this(null, null);
	}

	public MetadataWriter(ByteBufAllocator allocator, ByteBuf source) {
		this.allocator = allocator != null ? allocator : ByteBufAllocator.DEFAULT;
		if (source instanceof CompositeByteBuf)
			this._compositeByteBuf = (CompositeByteBuf) source;
		else if (source != null && source.readableBytes() != 0) {
			throw new IllegalArgumentException("MetadataWriter requires a CompositeByteBuf or an empty source ByteBuf");
		}
	}

	public CompositeByteBuf getCompositeByteBuf() {
		if (_compositeByteBuf != null)
			return _compositeByteBuf;
		synchronized (this) {
			if (_compositeByteBuf == null)
				_compositeByteBuf = this.allocator.compositeBuffer();
		}
		return _compositeByteBuf;
	}

	public void writeString(MimeType mimeType, String... values) {
		write(mimeType, values == null ? Stream.empty() : Arrays.asList(values).stream(),
				s -> s.map(IPCUtils::byteBufFromString));
	}

	public void writeEntries(MimeType mimeType, String... keyValueEntries) {
		Map<String, Collection<String>> parameterMap;
		if (keyValueEntries == null || keyValueEntries.length == 0)
			parameterMap = Collections.emptyMap();
		else {
			parameterMap = new LinkedHashMap<>();
			OfInt iter = IntStream.range(0, keyValueEntries.length).iterator();
			String key = null;
			while (iter.hasNext()) {
				int index = iter.nextInt();
				boolean even = index % 2 == 0;
				boolean end = !iter.hasNext();
				String str = keyValueEntries[index];
				if (!even || end) {
					String value = !even ? str : null;
					parameterMap.computeIfAbsent(key, nil -> new ArrayList<>()).add(value);
				} else
					key = str;

			}
		}
		writeEntries(mimeType, parameterMap);
	}

	public void writeEntries(MimeType mimeType, Map<String, ? extends Iterable<String>> parameterMap) {
		if (parameterMap == null || parameterMap.isEmpty())
			return;
		Stream<Stream<Entry<String, String>>> streams = parameterMap.entrySet().stream().map(ent -> {
			Iterable<String> value = ent.getValue();
			if (value == null)
				return Stream.empty();
			Stream<Entry<String, String>> stream = IPCUtils.stream(value.iterator())
					.map(v -> new SimpleImmutableEntry<>(ent.getKey(), v));
			return stream;
		});
		writeEntries(mimeType, IPCUtils.flatMap(streams));
	}

	public void writeEntries(MimeType mimeType, Stream<? extends Entry<String, String>> stream) {
		if (stream == null)
			return;
		stream = stream.filter(Objects::nonNull);
		stream = stream.filter(e -> IPCUtils.nonEmpty(e.getKey()));
		stream = stream.map(e -> {
			if (e.getValue() != null)
				return e;
			return new SimpleImmutableEntry<>(e.getKey(), "");
		});
		write(mimeType, stream, s -> {
			String query = IPCUtils.encodeEntries(s);
			return Stream.of(IPCUtils.byteBufFromString(query));
		});
	}

	public <X> void write(MimeType mimeType, Stream<X> valueStream, Function<Stream<X>, Stream<ByteBuf>> encoder) {
		Objects.requireNonNull(mimeType);
		Objects.requireNonNull(valueStream);
		Objects.requireNonNull(encoder);
		Stream<ByteBuf> stream = encoder.apply(valueStream);
		if (stream == null)
			return;
		Optional<WellKnownMimeType> wellKnownMimeTypeOp = mimeType.getWellKnownMimeType();
		stream.forEach(bb -> {
			if (wellKnownMimeTypeOp.isPresent())
				CompositeMetadataCodec.encodeAndAddMetadata(getCompositeByteBuf(), allocator,
						wellKnownMimeTypeOp.get(), bb);
			else
				CompositeMetadataCodec.encodeAndAddMetadata(getCompositeByteBuf(), allocator, mimeType.getString(),
						bb);
		});

	}

}
