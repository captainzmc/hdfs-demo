package com.tencent.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;

import java.io.IOException;
import java.util.Arrays;

public class TestReadFS {

//    private static final String HDFS_URI = "hdfs://localhost:9000";
//    private static final String FILE_PATH = "/readtest/testfile";
//    private static final int BUFFER_SIZE = 64 * 1024;
    private static final int LEN_64_MB = 64 * 1024 * 1024;
    public static void main(String[] args) throws IOException, InterruptedException {
        String HDFS_URI = args[0];
        String FILE_PATH = args[1];
        int BUFFER_SIZE = Integer.parseInt(args[2]);
        Configuration conf = new HdfsConfiguration();
        conf.set("fs.defaultFS", HDFS_URI);
        FileSystem fs = FileSystem.get(conf);
        System.out.println("START: read;" + FILE_PATH);

        final int runTimes = 15;
        long[] usedTimeArr = new long[runTimes];
        for (int i = 0; i < 15; i++) {
            long startTime = System.currentTimeMillis();
            try {
                try (FSDataInputStream in = fs.open(new Path(FILE_PATH))) {

                    byte[] buffer = new byte[BUFFER_SIZE];
                    int bytesRead;

                    int len = 0;
                    while ((bytesRead = in.read(buffer)) > 0) {
                        len += bytesRead;
//                        processData(buffer, bytesRead);
                        if (len > LEN_64_MB) {
                            break;
                        }
                    }
                } finally {

                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            usedTimeArr[i] = System.currentTimeMillis() - startTime;
        }

        System.out.println("DATA: " + Arrays.toString(usedTimeArr));

        long maxTime = Arrays.stream(usedTimeArr).max().getAsLong();
        double avgTime = Arrays.stream(usedTimeArr).average().getAsDouble();
        long minTime = Arrays.stream(usedTimeArr).min().getAsLong();

        Arrays.sort(usedTimeArr);
        long midTime = usedTimeArr[runTimes / 2];

        System.out.println("EXEC: " + runTimes
                + ", MAX: " + maxTime + ", MIN: " + minTime + ", AVG: " + avgTime + ", MID: " + midTime);

    }
}