package io.rsocket.ipc.encoders;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import io.netty.buffer.ByteBuf;
import io.rsocket.ipc.mimetype.MimeType;
import io.rsocket.ipc.mimetype.MimeTypes;
import io.rsocket.ipc.util.IPCUtils;
import io.rsocket.metadata.CompositeMetadata;

public class MetadataReader {

	private final CompositeMetadata compositeMetadata;

	public MetadataReader(ByteBuf source) {
		this(source, false);
	}

	public MetadataReader(ByteBuf source, boolean retainSlices) {
		Objects.requireNonNull(source);
		this.compositeMetadata = new CompositeMetadata(source, retainSlices);
	}

	public boolean containsString(MimeType mimeType, String value) {
		return containsString(mimeType, value, false, -1);
	}

	public boolean containsStringSecure(MimeType mimeType, String value) {
		Objects.requireNonNull(mimeType);
		IPCUtils.requireNonEmpty(value);
		return containsString(mimeType, value, false, 1);
	}

	public boolean containsString(MimeType mimeType, String value, boolean ignoreCase, int maxCandidates) {
		Stream<String> stream = streamStrings(mimeType);
		if (maxCandidates != -1)// ex: limit password candidates
			stream = stream.limit(maxCandidates);
		stream = stream.filter(v -> IPCUtils.equals(v, value, ignoreCase));
		return stream.findFirst().isPresent();
	}

	public boolean containsEntry(MimeType mimeType, String key, String value) {
		return containsEntry(mimeType, key, value, false, -1);
	}

	public boolean containsEntry(MimeType mimeType, String key, String value, boolean ignoreCase, int maxCandidates) {
		Stream<Entry<String, Optional<String>>> stream = streamEntries(mimeType)
				.filter(e -> IPCUtils.equals(e.getKey(), key, ignoreCase));
		if (maxCandidates != -1)// ex: limit password candidates
			stream = stream.limit(maxCandidates);
		stream = stream.filter(e -> IPCUtils.equals(e.getValue().orElse(null), value, ignoreCase));
		return stream.findFirst().isPresent();
	}

	public Stream<String> streamStrings(MimeType mimeType) {
		return stream(toTest -> Objects.equals(toTest, mimeType), e -> {
			return Stream.of(IPCUtils.byteBufToString(e.getContent()));
		});
	}

	public Stream<String> streamStringsNonEmpty(MimeType mimeType) {
		return streamStrings(mimeType).filter(v -> !IPCUtils.isNullOrEmpty(v));
	}

	public Stream<Entry<String, Optional<String>>> streamEntries(MimeType mimeType) {
		return stream(toTest -> Objects.equals(toTest, mimeType), e -> {
			return IPCUtils.decodeEntries(IPCUtils.byteBufToString(e.getContent()));
		});
	}

	public Map<String, List<Optional<String>>> getEntries(MimeType mimeType) {
		Map<String, List<Optional<String>>> map = new LinkedHashMap<>();
		streamEntries(mimeType)
				.forEach(e -> map.computeIfAbsent(e.getKey(), nil -> new ArrayList<>()).add(e.getValue()));
		return map;
	}

	public Stream<Entry<String, String>> streamEntriesNonEmpty(MimeType mimeType) {
		return streamEntries(mimeType).filter(e -> e.getValue().isPresent())
				.map(e -> new SimpleImmutableEntry<>(e.getKey(), e.getValue().get()))
				.filter(e -> !IPCUtils.isNullOrEmpty(e.getValue())).map(v -> v);
	}

	public Map<String, List<String>> getEntriesNonEmpty(MimeType mimeType) {
		Map<String, List<String>> map = new LinkedHashMap<>();
		streamEntriesNonEmpty(mimeType)
				.forEach(e -> map.computeIfAbsent(e.getKey(), nil -> new ArrayList<>()).add(e.getValue()));
		return map;
	}

	public <X> Stream<X> stream(Predicate<MimeType> mimeTypePredicate,
			Function<io.rsocket.metadata.CompositeMetadata.Entry, Stream<X>> decoder) {
		Objects.requireNonNull(mimeTypePredicate);
		Objects.requireNonNull(decoder);
		Stream<Stream<X>> streams = this.getCompositeMetadata().stream().filter(e -> {
			if (IPCUtils.isNullOrEmpty(e.getMimeType()))
				return false;
			MimeType mimteType = MimeTypes.create(e.getMimeType());
			return mimeTypePredicate.test(mimteType);
		}).map(decoder);
		return IPCUtils.flatMap(streams);
	}

	public CompositeMetadata getCompositeMetadata() {
		return compositeMetadata;
	}

}
