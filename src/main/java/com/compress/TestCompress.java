package com.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

public class TestCompress {
    public static void main(String[] args) throws IOException {
//        compress("C:\\Users\\IGARASHI\\Desktop\\MapReducer\\233.txt", BZip2Codec.class);
        depress("C:\\Users\\IGARASHI\\Desktop\\MapReducer\\233.txt.bz2");
    }

    private static void depress(String path) throws IOException {
        CompressionCodecFactory factory = new CompressionCodecFactory(new Configuration());
        CompressionCodec codec = factory.getCodec(new Path(path));

        CompressionInputStream cis = codec.createInputStream(new FileInputStream(path));
        FileOutputStream fos = new FileOutputStream(path.substring(0, path.length() - 4));

        IOUtils.copyBytes(cis, fos, 1024);

        IOUtils.closeStream(cis);
        IOUtils.closeStream(fos);
    }

    private static void compress(String path, Class<? extends CompressionCodec> codecClass) throws IOException {
        FileInputStream fis = new FileInputStream(path);
        CompressionCodec codec = ReflectionUtils.newInstance(codecClass, new Configuration());
        FileOutputStream fos = new FileOutputStream(path + codec.getDefaultExtension());

        CompressionOutputStream cos = codec.createOutputStream(fos);

        IOUtils.copyBytes(fis, cos, 1024);

        IOUtils.closeStream(fis);
        IOUtils.closeStream(cos);
    }
}
