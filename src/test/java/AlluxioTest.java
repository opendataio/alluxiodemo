import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.security.AccessController;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import sun.security.action.GetPropertyAction;

public class AlluxioTest {

  private static final File tmpdir = new File(AccessController
      .doPrivileged(new GetPropertyAction("java.io.tmpdir")));
  private static final java.nio.file.Path destFilePath = Paths.get(tmpdir.getAbsolutePath(),
      "hcfstest" + System.currentTimeMillis() + ".txt");
  private static final String content = "A cat will append to the end of the file";
  private File file;
  Path alluxioPath = new Path("alluxio:///tmp/test");
  private Configuration conf;

  @Before
  public void setUpClass() {
    conf = new Configuration();
//    conf.set("fs.alluxio.impl", alluxio.hadoop.FileSystem.class.getName());
    try {
      this.file = File.createTempFile("hcfstest", ".txt");
      if (!file.exists()) {
        file.createNewFile();
      }

      FileWriter fileWritter = new FileWriter(file.getAbsolutePath(), true);
      fileWritter.write(content);
      fileWritter.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @After
  public void tearDown() throws IOException {
    FileUtils.deleteQuietly(file);
    FileUtils.deleteQuietly(destFilePath.toFile());
    conf = null;
  }


  @Test
  public void testLs() throws IOException {
    Path path = new Path("alluxio:///");
    
    FileSystem fs = path.getFileSystem(conf);
    FileStatus[] statusArray = fs.listStatus(path);
    Assert.assertNotEquals(0, statusArray.length);
  }

  @Test
  public void testReadAndWrite() throws IOException {
    Path localSrcPath = new Path(file.getAbsolutePath());
    Path localDestPath = new Path(destFilePath.toString());
    FileUtil
        .copy(localSrcPath.getFileSystem(conf), localSrcPath,
            alluxioPath.getFileSystem(conf), alluxioPath,
            false, conf);
    FileUtil
        .copy(alluxioPath.getFileSystem(conf), alluxioPath,
            localDestPath.getFileSystem(conf), localDestPath,
            false, conf);
    try (FSDataInputStream isLocalSrc = localSrcPath.getFileSystem(conf).open(localSrcPath);
        FSDataInputStream isAlluxio = alluxioPath.getFileSystem(conf).open(alluxioPath);
        FSDataInputStream isLocalDest = localDestPath.getFileSystem(conf).open(localDestPath)) {
      String md5 = DigestUtils.md5Hex(isLocalSrc);
      String md52 = DigestUtils.md5Hex(isAlluxio);
      String md53 = DigestUtils.md5Hex(isLocalDest);
      Assert.assertEquals(md5, md52);
      Assert.assertEquals(md5, md53);
    }
  }
}
