package onememanyus.embedded;

import org.junit.Test;

public class EmbeddedKafkaTests {

	@Test
	public void testServerStart() throws Exception {
		EmbeddedZookeeper zk = new EmbeddedZookeeper();
		EmbeddedKafka     kafka = new EmbeddedKafka(zk);
		
		kafka.close();
		zk.close();
		
	}
	
}
