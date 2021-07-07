package io.rsocket.ipc.reflection;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.function.BiFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.google.gson.Gson;

import io.netty.buffer.ByteBuf;
import io.rsocket.RSocket;
import io.rsocket.core.RSocketConnector;
import io.rsocket.ipc.encoders.MetadataEncoderLFP;
import io.rsocket.ipc.marshallers.GsonMarshaller;
import io.rsocket.ipc.marshallers.GsonUnmarshaller;
import io.rsocket.ipc.reflection.client.RSocketIPCClients;
import io.rsocket.transport.netty.client.TcpClientTransport;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class ClientTest {

	public static void main(String[] args) {
		com.google.gson.Gson gson = new Gson();
		RSocket rsocket = RSocketConnector.create().connect(TcpClientTransport.create("localhost", 7000)).block();
		BiFunction<Type, ByteBuf, Object> deserializer = (t, bb) -> {
			return GsonUnmarshaller.create(gson, t).apply(bb);
		};
		TestServiceChannel client = RSocketIPCClients.create(() -> rsocket, TestServiceChannel.class,
				new MetadataEncoderLFP(), v -> {
					ByteBuf bb = GsonMarshaller.create(gson).apply(v);
					System.out.println(bb.toString(StandardCharsets.UTF_8));
					return bb;
				}, deserializer);
		System.out.println(client.addMono(69, 420).block());
		System.out.println(client.add(69, 420));
		client.intFlux(IntStream.range(0, 20).toArray()).toStream().forEach(v -> {
			System.out.println(v);
		});
		Stream<String> res = client.cool(Arrays.asList(new Date(), new Date(0l)), "sup");
		res.forEach(v -> {
			System.out.println(v);
		});

		Flux<String> resp = client.channel(Flux.range(0, 10).map(i -> new Date()).delayElements(Duration.ofSeconds(1)));
		resp.toStream().forEach(v -> {
			System.out.println(v);
		});
	}
}
