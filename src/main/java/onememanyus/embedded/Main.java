package onememanyus.embedded;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Main {
	public static void main(String...args) throws Exception {
		final EmbeddedZookeeper      zk       = new EmbeddedZookeeper();
		final EmbeddedKafka          kafka    = new EmbeddedKafka(zk);
		final EmbeddedSchemaRegistry registry = new EmbeddedSchemaRegistry(32001,zk.connectString());
		Runtime.getRuntime().addShutdownHook(new Thread() {

			@Override
			public void run() {
				try {
					registry.close();
				} catch(Exception e) {
					log.error("Could not shut down schema registry",e);
				}
				try {
					kafka.close();
				} catch(Exception e) {
					log.error("Could not shut down Kafka broker",e);
				}
				try {
					zk.close();
				} catch(Exception e) {
					log.error("Could not shut down Zookeeper",e);
				}
			}
			
		});
		log.info("Zookeeper Connect String: {}",zk.connectString());
		log.info("Kafka Connect String: {}",kafka.connectString());
		log.info("Schema Registry Connect String: {}",registry.connectString());
		zk.awaitShutdown();
	}
}
