package yuhuachang;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HdfsDownload {
    public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException {
        String filePath = "D:\\data\\air-from-hdfs.png";
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://ubuntu133:9000"), conf, "yuhuachang");
        FSDataInputStream inputStream = fs.open(new Path("/air.png"));
        FileOutputStream outputStream = new FileOutputStream(new File(filePath));
        IOUtils.copyBytes(inputStream, outputStream, conf, true);
        System.out.println( "ok!" );
    }
}
