package com.tencent.zmc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;

import java.io.IOException;
import java.net.URI;

public class DopMountTableFileSystem extends LocalFileSystem {

    public DopMountTableFileSystem() {
        try {
            initialize(URI.create("file:///"),  new HdfsConfiguration());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public FSDataInputStream open(Path f) throws IOException {

        return new FSDataInputStreamWithDopMetrics(super.open(f).getWrappedStream());
    }
}
