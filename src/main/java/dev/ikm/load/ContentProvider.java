package dev.ikm.load;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Supplier;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

/**
 * A way to pass around closeable input streams for doing imports without passing around all the zip file mess.
 * @author <a href="mailto:daniel.armbrust.list@sagebits.net">Dan Armbrust</a>
 */
public class ContentProvider implements Supplier<ContentStreamProvider>
{
    String streamSourceName;
    Supplier<ContentStreamProvider> contentStream;
    private Supplier<byte[]> itemByteSupplier;

    public ContentProvider(Path path)
    {
        this.streamSourceName = path.toString();
        this.contentStream = () -> new ContentStreamProvider()
        {
            @Override
            public void close() throws Exception
            {
                // noop
            }

            @Override
            public InputStream get()
            {
                try
                {
                    return Files.newInputStream(path);
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    public ContentProvider(File zipFile, ZipEntry entry)
    {
        this(zipFile, null, entry, null);
    }

    public ContentProvider(String streamSourceName, Supplier<byte[]> unzippedOuterBytes)
    {
        this.streamSourceName = streamSourceName;
        this.itemByteSupplier = unzippedOuterBytes;
        this.contentStream = () -> new ContentStreamProvider()
        {
            @Override
            public void close() throws Exception
            {
                itemByteSupplier = null;
            }

            @Override
            public InputStream get()
            {
                return new ByteArrayInputStream(ContentProvider.this.itemByteSupplier.get());
            }
        };
    }

    //TODO replace this constructor with the approach that uses the supplier constructor, to keep all of the zip
    // handling logic in the place where it comes from, and supports paths, instead of just files.
    @Deprecated
    public ContentProvider(File zipFile, ZipEntry nestedZipFile, ZipEntry entry, byte[] unzippedOuterBytes)
    {
        this.itemByteSupplier = () -> unzippedOuterBytes;
        streamSourceName = (nestedZipFile == null ? (zipFile.getName() + ":" + entry.getName())
                : (zipFile.getName() + ":" + nestedZipFile.getName() + ":" + entry.getName()));

        contentStream = () -> new ContentStreamProvider()
        {
            private ZipFile zipFileHandle;

            @Override
            public void close() throws Exception
            {
                if (zipFileHandle != null)
                {
                    zipFileHandle.close();
                }
                ContentProvider.this.itemByteSupplier = null;
            }

            @Override
            public InputStream get()
            {
                try
                {
                    if (nestedZipFile == null)
                    {
                        zipFileHandle = new ZipFile(zipFile, Charset.forName("UTF-8"));
                        return zipFileHandle.getInputStream(entry);
                    }
                    else
                    {
                        if (ContentProvider.this.itemByteSupplier == null)
                        {
                            // This is returning a stream of another zip file.
                            zipFileHandle = new ZipFile(zipFile, Charset.forName("UTF-8"));
                            ZipInputStream innerZipStream = new ZipInputStream(zipFileHandle.getInputStream(nestedZipFile), Charset.forName("UTF-8"));
                            ZipEntry innerZipEntry = innerZipStream.getNextEntry();
                            while (innerZipEntry != null)
                            {
                                if (innerZipEntry.getName().equals(entry.getName()))
                                {
                                    return innerZipStream;
                                }
                                innerZipEntry = innerZipStream.getNextEntry();
                            }
                            throw new RuntimeException("Wrong file names passed in");
                        }
                        else
                        {
                            return new ByteArrayInputStream(ContentProvider.this.itemByteSupplier.get());
                        }
                    }
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    public String getStreamSourceName()
    {
        return streamSourceName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ContentStreamProvider get()
    {
        return contentStream.get();
    }
}