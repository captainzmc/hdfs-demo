package com.tencent.zmc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;

public class DopClientMetricsUtils {
    public static String getMetrics(FSDataInputStream is) {
        try {
            String metrics = (String) is.getClass().getMethod("getMetrics").invoke(is);
            return metrics;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    public static void main(String[] arge) throws IOException {

        DopMountTableFileSystem fs = new DopMountTableFileSystem();
        FSDataInputStream inputStream = fs.open(new Path("/Users/micahzhao/zmc/IdeaProject/hdfs-demo/pom.xml"));
        // 通过反射获取监控指标
        String metrics = DopClientMetricsUtils.getMetrics(inputStream);
        System.out.println("Metrics: " + metrics); // 输出: Metrics: mockMetrics

    }
}