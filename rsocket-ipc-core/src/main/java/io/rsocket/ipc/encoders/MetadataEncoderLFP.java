package io.rsocket.ipc.encoders;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Stream;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.opentracing.SpanContext;
import io.rsocket.ipc.MetadataEncoder;
import io.rsocket.ipc.decoders.MetadataWriter;
import io.rsocket.ipc.mimetype.MimeTypes;
import io.rsocket.ipc.util.IPCUtils;
import io.rsocket.ipc.util.IPCUtils.DisposableAddList;
import reactor.core.Disposable;

public class MetadataEncoderLFP implements MetadataEncoder {

	public static interface Interceptor extends Consumer<MetadataWriter> {
	};

	private final ByteBufAllocator allocator;
	private final DisposableAddList<MetadataEncoderLFP.Interceptor> interceptors = DisposableAddList.create();

	public MetadataEncoderLFP(MetadataEncoderLFP.Interceptor... interceptors) {
		this(ByteBufAllocator.DEFAULT, interceptors);
	}

	public MetadataEncoderLFP(ByteBufAllocator allocator, MetadataEncoderLFP.Interceptor... interceptors) {
		this.allocator = Objects.requireNonNull(allocator);
		if (interceptors != null)
			Arrays.asList(interceptors).stream().filter(Objects::nonNull).forEach(v -> this.addInterceptor(v));
	}

	public Disposable addInterceptor(MetadataEncoderLFP.Interceptor interceptor) {
		Objects.requireNonNull(interceptor);
		return interceptors.disposableAdd(interceptor);
	}

	@Override
	public final ByteBuf encode(ByteBuf metadata, SpanContext spanContext, String service, String... parts) {
		MetadataWriter metadataWriter = new MetadataWriter(this.allocator, metadata);
		this.writeMetadata(metadataWriter, spanContext, service, parts);
		return metadataWriter.getCompositeByteBuf();
	}

	protected void writeMetadata(MetadataWriter metadataWriter, SpanContext spanContext, String service,
			String... parts) {
		interceptors.forEach(interceptor -> interceptor.accept(metadataWriter));
		writeRoutingInfo(metadataWriter, service, parts);
		writeTracingSpanContext(metadataWriter, spanContext);
	}

	private void writeRoutingInfo(MetadataWriter metadataWriter, String service, String... parts) {
		metadataWriter.writeString(MimeTypes.MIME_TYPE_SERVICE, service);
		Stream<String> methodsStream = parts == null ? Stream.empty()
				: Arrays.asList(parts).stream().filter(IPCUtils::nonEmpty);
		methodsStream.forEach(v -> {
			metadataWriter.writeString(MimeTypes.MIME_TYPE_METHOD, v);
		});
	}

	private void writeTracingSpanContext(MetadataWriter metadataWriter, SpanContext spanContext) {
		if (spanContext == null)
			return;
		Iterable<Entry<String, String>> items = spanContext.baggageItems();
		if (items == null)
			return;
		Map<String, Collection<String>> paramMap = new LinkedHashMap<>();
		for (Entry<String, String> ent : items)
			paramMap.computeIfAbsent(ent.getKey(), nil -> new LinkedHashSet<>()).add(ent.getValue());
		metadataWriter.writeEntries(MimeTypes.MIME_TYPE_TRACER, paramMap);
	}
}
