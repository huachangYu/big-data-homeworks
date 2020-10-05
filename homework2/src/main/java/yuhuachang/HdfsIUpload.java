package yuhuachang;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HdfsIUpload {
    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException
    {
        String filePath = "D:\\data\\air.png";
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://ubuntu133:9000"), conf, "yuhuachang");
        FSDataOutputStream outputStream = fs.create(new Path("/air.png"));
        FileInputStream inputStream = new FileInputStream(filePath);
        IOUtils.copyBytes(inputStream, outputStream, conf, true);
        System.out.println( "ok!" );
    }
}
