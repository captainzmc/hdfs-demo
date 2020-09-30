package com;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TestOFS {
    FileSystem hdfs = null;

    public void createFile(String newpath,String file, int num) throws Exception{
        FileSystem fSys = null;
        String path = "ofs://9.29.167.116/";
        URI uri = new URI(path);
        Configuration ozoneConfig = new Configuration();
        fSys = FileSystem.get(uri, ozoneConfig,"root");
        System.out.println("====start:"+new Date().toString());
        for(int i=0; i< num; i++) {
            fSys.createNewFile(new Path("ofs://9.29.167.116/"+newpath+"/"+file+i));
        }
        System.out.println("====stop:"+new Date().toString());
    }

    public void createFile(String newpath,String file, int num, String str) throws Exception{
        System.out.println(new Date().toString());
        String dst = "ofs://9.29.167.116/";

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(dst), conf,"root");
        for(int i=0;i<num;i++){
            FSDataOutputStream fsOut = fs.create(new Path("ofs://9.29.167.116/"+newpath+"/"+file+i), true);
            fsOut.write(str.getBytes());
            fsOut.flush();
            fsOut.close();
        }

        System.out.println(new Date().toString());
    }

    public void TestlistStatus(String newpath) throws Exception {

        FileSystem fSys = null;
        String path = "ofs://9.29.167.116/";
        URI uri = new URI(path);
        Configuration ozoneConfig = new Configuration();
        fSys = FileSystem.get(uri, ozoneConfig,"root");

        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        Long starta = System.currentTimeMillis();
        String start = formatter.format(starta);
        FileStatus[] status = fSys.listStatus(new Path("ofs://9.29.167.116/"+newpath));
        System.out.println("status size:"+status.length);

        Long enda = System.currentTimeMillis();
        String end = formatter.format(enda);
        System.out.println("start:"+start);
        System.out.println("end:"+end);
        System.out.println("cost Millis:"+(enda-starta));
    }




    public void renameFile(String src, String dst)throws Exception{
        Configuration conf = new Configuration();
        String path = "ofs://9.29.167.116";
        FileSystem fs = FileSystem.get(URI.create(path), conf,"root");
        try{
            Path srcPath = new Path("ofs://9.29.167.116/"+src);
            Path dstPath = new Path("ofs://9.29.167.116/"+dst);
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            Long starta = System.currentTimeMillis();
            String start = formatter.format(starta);
            fs.rename(srcPath, dstPath);
            Long enda = System.currentTimeMillis();
            String end = formatter.format(enda);
            System.out.println("start:"+start);
            System.out.println("end:"+end);
            System.out.println("cost Millis:"+(enda-starta));
        }catch(Exception ex){
            ex.printStackTrace();
        }finally{
            if(fs != null){
                try {
                    fs.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
    }

    public void deleteFile(String dirName) throws Exception{
        Configuration conf = new Configuration();
        String path = "ofs://9.29.167.116/"+dirName;

        FileSystem fs = FileSystem.get(URI.create(path), conf,"root");
        try{
            Long starta = System.currentTimeMillis();
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            String start = formatter.format(starta);
            Path srcPath = new Path(path);
            fs.delete(srcPath,true);
            Long enda = System.currentTimeMillis();
            String end = formatter.format(enda);
            System.out.println("start:"+start);
            System.out.println("end:"+end);
            System.out.println("cost Millis:"+(enda-starta));
        }catch(Exception ex){
            ex.printStackTrace();
        }finally{
            if(fs != null){
                try {
                    fs.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) {

        TestOFS learn = new TestOFS();
//        System.out.println(DigestUtils.md5Hex("12345678"));
        try {
            if (args.length > 0) {
                if (args[0].equals("delete")) {
                    learn.deleteFile(args[1]);
                    System.out.println("delete file");
                } else {
                    if (args.length == 3) {
                        learn.createFile(args[0], args[1], Integer.parseInt(args[2]));
                    }
                    if (args.length == 4) {
                        learn.createFile(args[0], args[1], Integer.parseInt(args[2]), args[3]);
                    }
                    if (args.length == 1) {
                        learn.TestlistStatus(args[0]);
                    }
                    if (args.length == 2) {
                        learn.renameFile(args[0], args[1]);
                    }
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
