package com.example.RSocket;

import io.rsocket.transport.netty.client.TcpClientTransport;
import lombok.AllArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.http.codec.cbor.Jackson2CborDecoder;
import org.springframework.http.codec.cbor.Jackson2CborEncoder;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.messaging.rsocket.RSocketStrategies;
import org.springframework.stereotype.Component;
import org.springframework.util.MimeType;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.time.Instant;

@SpringBootApplication
public class RSocketApplication {

	public static void main(String[] args) {
		SpringApplication.run(RSocketApplication.class, args);
	}

}

enum Status {

	CHUNK_COMPLETED,
	COMPLETED,
	FAILED;

}

class Constants {

	public static final String MIME_FILE_EXTENSION   = "message/x.upload.file.extension";
	public static final String MIME_FILE_NAME        = "message/x.upload.file.name";
	public static final String FILE_NAME = "file-name";
	public static final String FILE_EXTN = "file-extn";

}

@Configuration
class ClientConfig {

	@Bean
	public RSocketStrategies rSocketStrategies() {
		return RSocketStrategies.builder()
				.encoders(encoders -> encoders.add(new Jackson2CborEncoder()))
				.decoders(decoders -> decoders.add(new Jackson2CborDecoder()))
				.metadataExtractorRegistry(metadataExtractorRegistry -> {
					metadataExtractorRegistry.metadataToExtract(MimeType.valueOf(Constants.MIME_FILE_EXTENSION), String.class, Constants.FILE_EXTN);
					metadataExtractorRegistry.metadataToExtract(MimeType.valueOf(Constants.MIME_FILE_NAME), String.class, Constants.FILE_NAME);
				})
				.build();
	}

	@Bean
	public Mono<RSocketRequester> getRSocketRequester(RSocketRequester.Builder builder){
		return builder
				.rsocketConnector(rSocketConnector -> rSocketConnector.reconnect(Retry.fixedDelay(2, Duration.ofSeconds(2))))
				.connect(TcpClientTransport.create(6565));
	}

}

@Component
class FileUploadClient {

	private Mono<RSocketRequester> rSocketRequester;

	@Value("classpath:input/java_tutorial.pdf")
	private Resource resource;

	public void uploadFile() {
		System.out.println(resource.toString());
		// read input file as 4096 chunks
		Flux<DataBuffer> readFlux = DataBufferUtils.read(resource, new DefaultDataBufferFactory(), 4096)
				.doOnNext(s -> System.out.println("Sent"));

		// rsocket request
		this.rSocketRequester
				.map(r -> r.route("file.upload")
						.metadata(metadataSpec -> {
							metadataSpec.metadata("pdf", MimeType.valueOf(Constants.MIME_FILE_EXTENSION));
							metadataSpec.metadata("output", MimeType.valueOf(Constants.MIME_FILE_NAME));
						})
						.data(readFlux)
				)
				.flatMapMany(r -> r.retrieveFlux(Status.class))
				.doOnNext(s -> System.out.println("Upload Status : " + s))
				.subscribe();
	}
}



