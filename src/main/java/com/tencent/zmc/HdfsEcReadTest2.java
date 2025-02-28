package com.tencent.zmc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class HdfsEcReadTest2 {

    private static final String HDFS_URI = "hdfs://th-tdw-test-2-v3";
    private static final String FILE_PATH = "/readtest/testfile";

    public static void main(String[] args) throws IOException, InterruptedException {
        Configuration conf = new HdfsConfiguration();
        conf.set("fs.defaultFS", HDFS_URI);
        int index = Integer.parseInt(args[0]);
        int start = Integer.parseInt(args[2]);
        FileSystem fs = FileSystem.get(conf);

        try {
            for (int i = 0; i < 5; i++) {
                for(int j = start; j < start + 5; j++) {
                    if (j==index) {
                        System.out.println("=====start read=====" + FILE_PATH+ j);
                    }
                    long startTime = System.currentTimeMillis();
                    Path filePath = new Path(FILE_PATH+ j);
                    FSDataInputStream inputStream =  fs.open(filePath);
                    byte[] buffer = new byte[1024 * 1024]; // 1MB 缓冲区
                    int readLen = -1;
                    int totalReadLen = 0;
                    do {
                        readLen = inputStream.read(buffer);
                        totalReadLen += readLen;
                    } while (readLen >= 0);
                    long totalreadtime =System.currentTimeMillis()-startTime;
                    if (j==index) {
                        System.out.println(FILE_PATH+ j+"=====total read=====" + totalReadLen +"====total time:"+totalreadtime);
                    }
                    inputStream.close();
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        fs.close();

    }
}