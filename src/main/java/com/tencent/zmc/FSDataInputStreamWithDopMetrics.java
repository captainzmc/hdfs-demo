package com.tencent.zmc;

import org.apache.hadoop.fs.FSDataInputStream;

import java.io.InputStream;

public class FSDataInputStreamWithDopMetrics extends FSDataInputStream {
    public FSDataInputStreamWithDopMetrics(InputStream in) {
        super(in);
    }

    public String getMetrics() {
        return "======mockMetrics=======";
    }
}
