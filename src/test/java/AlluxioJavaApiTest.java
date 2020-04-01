import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.InstancedConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.OpenFilePOptions;
import alluxio.grpc.ReadPType;
import alluxio.grpc.WritePType;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.security.AccessController;
import java.util.List;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import sun.security.action.GetPropertyAction;

public class AlluxioJavaApiTest {

  private static final File tmpdir = new File(AccessController
      .doPrivileged(new GetPropertyAction("java.io.tmpdir")));
  private static final java.nio.file.Path destFilePath = Paths.get(tmpdir.getAbsolutePath(),
      "hcfstest" + System.currentTimeMillis() + ".txt");
  private static final String content = "A cat will append to the end of the file";
  private File file;
  AlluxioURI path = new AlluxioURI("/tmp/test");
  private FileSystem fs = FileSystem.Factory.get();
  private Configuration hcfsConf;

  @Before
  public void setUpClass() throws IOException, AlluxioException {
    hcfsConf = new Configuration();
    
    InstancedConfiguration alluxioConf = InstancedConfiguration.defaults();
    fs = FileSystem.Factory.create(alluxioConf);
    
    this.file = File.createTempFile("hcfstest", ".txt");
    if (!file.exists()) {
      file.createNewFile();
    }

    FileWriter fileWritter = new FileWriter(file.getAbsolutePath(), true);
    fileWritter.write(content);
    fileWritter.close();
    
    if (fs.exists(path)) {
      fs.delete(path);
    }
  }

  @After
  public void tearDown() throws IOException {
    FileUtils.deleteQuietly(file);
    FileUtils.deleteQuietly(destFilePath.toFile());
    fs.close();
    fs = null;
  }


  @Test
  public void testLs() throws IOException, AlluxioException {
    List<URIStatus> statusList = fs.listStatus(new AlluxioURI("/"));
    Assert.assertNotEquals(0, statusList);
  }

  @Test
  public void testReadAndWrite() throws IOException, AlluxioException {
    Path localSrcPath = new Path(file.getAbsolutePath());
    Path localDestPath = new Path(destFilePath.toString());

    try (FSDataInputStream in = localSrcPath.getFileSystem(hcfsConf).open(localSrcPath);
        FileOutStream out = fs.createFile(path,
            CreateFilePOptions.newBuilder().setWriteType(WritePType.MUST_CACHE).build())) {
      IOUtils.copy(in, out);
    }

    try (FileInStream in = fs.openFile(path,
        OpenFilePOptions.newBuilder().setReadType(ReadPType.NO_CACHE).build());
        FSDataOutputStream out = localDestPath.getFileSystem(hcfsConf).create(localDestPath)) {
      IOUtils.copy(in, out);
    }

    try (FSDataInputStream isLocalSrc = localSrcPath.getFileSystem(hcfsConf).open(localSrcPath);
        FileInStream isAlluxio = fs.openFile(path);
        FSDataInputStream isLocalDest = localSrcPath.getFileSystem(hcfsConf).open(localSrcPath)) {
      String md5 = DigestUtils.md5Hex(isLocalSrc);
      String md52 = DigestUtils.md5Hex(isAlluxio);
      String md53 = DigestUtils.md5Hex(isLocalDest);
      Assert.assertEquals(md5, md52);
      Assert.assertEquals(md5, md53);
    }
  }
}
