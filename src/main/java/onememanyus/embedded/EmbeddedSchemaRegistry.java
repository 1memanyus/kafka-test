package onememanyus.embedded;

import io.confluent.kafka.schemaregistry.RestApp;

public class EmbeddedSchemaRegistry implements EmbeddedServer {

	final RestApp server;
	
	public EmbeddedSchemaRegistry(int port,String zkConnectString) throws Exception {
		server = new RestApp(port,zkConnectString,"_schemas");
		server.start();
	}
	
	
	@Override
	public void close() throws Exception {
		server.stop();
	}

	@Override
	public String connectString() {
		return server.restConnect;
	}

}
