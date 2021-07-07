package io.rsocket.ipc.reflection.client;

import java.util.Objects;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public abstract class LazyRSocket implements RSocket {

	public static LazyRSocket create(Supplier<RSocket> rsocketSupplier) {
		Objects.requireNonNull(rsocketSupplier);
		return new LazyRSocket() {

			@Override
			protected RSocket getRSocket() {
				return rsocketSupplier.get();
			}
		};
	}

	protected abstract RSocket getRSocket();

	@Override
	public Mono<Void> fireAndForget(Payload payload) {
		return getRSocket().fireAndForget(payload);
	}

	@Override
	public Mono<Payload> requestResponse(Payload payload) {
		return getRSocket().requestResponse(payload);
	}

	@Override
	public Flux<Payload> requestStream(Payload payload) {
		return getRSocket().requestStream(payload);
	}

	@Override
	public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
		return getRSocket().requestChannel(payloads);
	}

	@Override
	public Mono<Void> metadataPush(Payload payload) {
		return getRSocket().metadataPush(payload);
	}

	@Override
	public double availability() {
		return getRSocket().availability();
	}

	@Override
	public Mono<Void> onClose() {
		return getRSocket().onClose();
	}

	@Override
	public void dispose() {
		getRSocket().dispose();
	}

	@Override
	public boolean isDisposed() {
		return getRSocket().isDisposed();
	}

}
