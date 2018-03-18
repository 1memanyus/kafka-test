package onememanyus.embedded;

import java.io.IOException;

import org.apache.curator.test.TestingServer;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EmbeddedZookeeper implements EmbeddedServer {
	
	final TestingServer server;
	
	public EmbeddedZookeeper() throws Exception {
		log.debug("Starting Zookeeper server");
		server = new TestingServer();
		log.debug("Zookeeper server started on {} using directory {}",server.getConnectString(),server.getTempDirectory());
	}

	public String connectString() {
		return server.getConnectString();
	}

	@Override
	public void close() throws Exception {
		log.debug("Stopping Zookeeper server at {}...",server.getConnectString());
		server.close();
		log.debug("Zookeeper server stopped.");		
	}
	
	
}
