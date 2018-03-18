package onememanyus.embedded;

import java.net.HttpURLConnection;
import java.net.URL;
import org.junit.Test;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EmbeddedKafkaTests {

	@Test
	public void testServerStart() throws Exception {
		EmbeddedZookeeper zk = new EmbeddedZookeeper();
		EmbeddedKafka     kafka = new EmbeddedKafka(zk);
		EmbeddedSchemaRegistry registry = new EmbeddedSchemaRegistry(32001,zk.connectString());
		URL url = new URL(registry.connectString()+"/subjects");
		HttpURLConnection conn = (HttpURLConnection)url.openConnection();
		log.info("Got response code {}",conn.getResponseCode());
		conn.disconnect();
		registry.close();
		kafka.close();
		zk.close();
		
	}
	
}
