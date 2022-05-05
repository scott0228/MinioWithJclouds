import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.stream.Collectors;

import com.google.common.io.ByteSource;

import org.apache.commons.io.IOUtils;
import org.jclouds.Constants;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.filesystem.reference.FilesystemConstants;
import org.jclouds.s3.blobstore.S3BlobStoreContext;
import org.junit.jupiter.api.Test;

public class TestJCloud {

  @Test
  void testFileSystem() throws IOException {
    BlobStoreContext context = createFileSystemContext();

    testContext(context);

  }

  private void testContext(BlobStoreContext context) throws IOException {
    String containerName = "uat-container";
    // create a container in the default location
    BlobStore blobStore = context.getBlobStore();
    blobStore.createContainerInLocation(null, containerName);

    // add blob
    // Create a Blob
    ByteSource payload = ByteSource.wrap(IOUtils.toByteArray(new FileInputStream("nginx.conf")));
    Blob blob = blobStore
        .blobBuilder("test/nginx.conf") //
        .payload(payload) //
        .contentLength(payload.size()) //
        .build();

    // Upload the Blob
    String eTag = blobStore.putBlob(containerName, blob);
    System.out.println(eTag);

    // retrieve blob
    Blob blobRetrieved = blobStore.getBlob(containerName, "test/nginx.conf");

    InputStream inputStream = blobRetrieved.getPayload().openStream();
    String text = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))
        .lines().collect(Collectors.joining("\n"));
    System.out.println(text);
    // delete blob
    // blobStore.removeBlob(containerName, "test.txt");

    // close context
    context.close();
  }

  private BlobStoreContext createFileSystemContext() {
    // setup where the provider must store the files
    Properties properties = new Properties();
    properties.setProperty(FilesystemConstants.PROPERTY_BASEDIR, "./local/filesystemstorage");
    // setup the container name used by the provider (like bucket in S3)

    // get a context with filesystem that offers the portable BlobStore api
    BlobStoreContext context = ContextBuilder.newBuilder("filesystem").overrides(properties)
        .buildView(BlobStoreContext.class);
    return context;
  }

  private BlobStoreContext createS3Context() {
    String identity = "minio";
    String credentials = "minio123";

    Properties properties = new Properties();
    properties.setProperty(Constants.PROPERTY_ENDPOINT, "http://172.16.42.112:9000");
    BlobStoreContext context = ContextBuilder.newBuilder("s3").credentials(identity, credentials).overrides(properties)
        .buildView(S3BlobStoreContext.class);
    return context;
  }

  @Test
  void testS3() throws IOException {
    BlobStoreContext context = createS3Context();

    testContext(context);
  }
}
