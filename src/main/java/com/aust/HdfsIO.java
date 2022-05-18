package com.aust;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
public class HdfsIO {

    @Test
    public void putFileToHdfs() throws URISyntaxException, IOException, InterruptedException {

        // 1 获取对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://master:9000"), conf, "root");
        // 2 获取输入流
        FileInputStream fis = new FileInputStream(new File("d:/tmp.txt"));

        //3 获取输出流
        FSDataOutputStream fdos = fs.create(new Path("/hdfstmp.txt"));
        //4 流的对拷
        IOUtils.copyBytes(fis, fdos, conf);
        //5 关闭资源
        IOUtils.closeStream(fdos);
        IOUtils.closeStream(fis);
        fs.close();

    }

    @Test
    public void getFileFromHdfs() throws URISyntaxException, IOException, InterruptedException {
//        1 获取对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://master:9000"), conf, "root");

//        2 获取输入流
        FSDataInputStream fis = fs.open(new Path("/banhua.txt"));
//        3 获取输出流
        FileOutputStream fos = new FileOutputStream(new File("d:/bh.txt"));
//        4 流对拷
        IOUtils.copyBytes(fis,fos,conf);
//        5 关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }

    @Test
    // 获取HDFS文件第一块
    public void getFirstBlock() throws URISyntaxException, IOException, InterruptedException {
        // 1 获取对象
        Configuration config = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://master:9000"), config, "root");
        // 2 获取输入流
        FSDataInputStream fis = fs.open(new Path("/SogouQ.reduced"));
        // 3 获取输出流
        FileOutputStream fos = new FileOutputStream(new File("d:/sr"));
        // 4 流对拷（只拷贝128m)
        byte[] buf = new byte[1024];
        for(int i = 0;i < 1024 * 128;i++){
            fis.read(buf);
            fos.write(buf);
        }
        // 5 关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();

    }

    @Test
    // 获取HDFS文件第二块
    public void getSeconeBlock() throws URISyntaxException, IOException, InterruptedException {
        // 1 获取对象
        Configuration config = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://master:9000"), config, "root");

        //2 获取输入流
        FSDataInputStream fis = fs.open(new Path("/SogouQ.reduced"));

        // 3 设置指定读取起点
        fis.seek(1024 * 1024 * 128);

        //4 获取输出流
        FileOutputStream fos = new FileOutputStream(new File("d:/sr2"));
        // 5 流对拷
        IOUtils.copyBytes(fis,fos,config);
        // 6 关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();

        /*
        在windows 中使用 type sr2 >> sr 将第二块内容拼接成到第二块中，然后改扩展名，就能得到原文件
        在Linux 中使用 cat sr2 >> sr
         */


    }


}

