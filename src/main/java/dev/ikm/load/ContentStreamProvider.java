package dev.ikm.load;

import java.io.InputStream;
import java.util.function.Supplier;

/**
 * A way to pass around closeable input streams for doing imports without passing around all the zip file mess.
 * @author <a href="mailto:daniel.armbrust.list@sagebits.net">Dan Armbrust</a>
 */
public interface ContentStreamProvider extends Supplier<InputStream>, AutoCloseable
{

}
