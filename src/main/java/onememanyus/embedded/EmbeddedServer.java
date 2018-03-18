package onememanyus.embedded;

import java.io.IOException;

public interface EmbeddedServer extends AutoCloseable {
	public String connectString();
}
