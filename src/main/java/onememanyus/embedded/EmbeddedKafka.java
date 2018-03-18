package onememanyus.embedded;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import org.apache.kafka.common.utils.Time;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.TestUtils;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EmbeddedKafka implements EmbeddedServer {
	
	final KafkaServer server;
	final Path        logDir = Files.createTempDirectory("kafkaembed");
	final KafkaConfig kafkaConfig;
	
	protected Properties mergeConfig(Properties config) {
		Properties merged = new Properties();
		
		merged.put(KafkaConfig.BrokerIdProp(),0);
		merged.put(KafkaConfig.HostNameProp(), "127.0.0.1");
		merged.put(KafkaConfig.PortProp(), "9092");
		merged.put(KafkaConfig.NumPartitionsProp(), 1);
		merged.put(KafkaConfig.AutoCreateTopicsEnableProp(), true);
		merged.put(KafkaConfig.MessageMaxBytesProp(), 1_000_000);
		merged.put(KafkaConfig.ControlledShutdownEnableProp(), true);
		
		merged.putAll(config);
		merged.put(KafkaConfig.LogDirProp(), logDir.toAbsolutePath().toString());
		return merged;
	}
	
	public EmbeddedKafka(Properties config) throws Exception {
		kafkaConfig = new KafkaConfig(mergeConfig(config),true);
		server = TestUtils.createServer(kafkaConfig, Time.SYSTEM);
	}

	public String connectString() {
		return kafkaConfig.hostName()+":"+kafkaConfig.port();
	}

	protected boolean deleteAll(Path path) throws IOException {
		if(Files.exists(path) && Files.isDirectory(path))
			Files.list(path).forEach((file) -> { try { if(Files.isDirectory(file)) deleteAll(file); else Files.delete(file); } catch(Exception e) { } });
		return Files.deleteIfExists(path);
	}
	
	@Override
	public void close() throws Exception {
		log.debug("Shutting down embedded kafka server");
		server.shutdown();
		server.awaitShutdown();
		log.debug("Deleting temporary files");
		deleteAll(logDir);
		log.debug("Embedded kafka server shut down");
	}
	
	
}
