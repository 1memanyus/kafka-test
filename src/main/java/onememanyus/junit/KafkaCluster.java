package onememanyus.junit;

import java.util.Properties;

import org.apache.curator.test.InstanceSpec;
import org.junit.rules.ExternalResource;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import onememanyus.embedded.EmbeddedKafka;
import onememanyus.embedded.EmbeddedSchemaRegistry;
import onememanyus.embedded.EmbeddedZookeeper;

@Slf4j
@RequiredArgsConstructor
public class KafkaCluster extends ExternalResource {
	
	final Properties brokerConfig;
	
	@Getter
	EmbeddedZookeeper zookeeper;
	
	@Getter
	EmbeddedKafka kafka;
	
	@Getter
	EmbeddedSchemaRegistry schemaRegistry;
	
	
	public KafkaCluster() {
		this(new Properties());
	}
	

	@Override
	protected void before() throws Throwable {
		super.before();
		log.debug("Starting embedded kafka cluster");
		if(zookeeper == null)
			zookeeper = new EmbeddedZookeeper();
		if(kafka == null)
			kafka     = new EmbeddedKafka(brokerConfig,zookeeper);
		if(schemaRegistry == null)
			schemaRegistry = new EmbeddedSchemaRegistry(InstanceSpec.getRandomPort(),zookeeper.connectString());
		log.debug("Embedded kafka cluster ready");
	}

	@Override
	protected void after() {
		log.debug("Shutting down kafka cluster");
		try {
			if(schemaRegistry != null)
				schemaRegistry.close();
		} catch(Exception e){
			log.error("Problem shutting down schema registry",e);
		}
		try {
			if(kafka != null)
				kafka.close();
		} catch(Exception e) {
			log.error("Problem shutting down kafka broker",e);
		}
		try {
			if(zookeeper != null)
				zookeeper.close();
		} catch(Exception e) {
			log.error("Problem shutting down Zookeeper",e);
		}
		log.debug("Kafka cluster shut down");
		super.after();
	}
	
	
}
