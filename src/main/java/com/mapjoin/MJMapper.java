package com.mapjoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class MJMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    Map<String, String> pdMap = new HashMap<>();
    Text k = new Text();

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {

        // 1 获取缓存的文件
        URI[] cacheFiles = context.getCacheFiles();
        String path = cacheFiles[0].getPath();

        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path)));

        String line;
        while(StringUtils.isNotEmpty(line = reader.readLine())){

            // 2 切割
            String[] fields = line.split("\t");

            // 3 缓存数据到集合
            pdMap.put(fields[0], fields[1]);
        }

        // 4 关流
        IOUtils.closeStream(reader);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 1 获取一行, 并截取
        String[] fields = value.toString().split("\t");

        // 2 获取商品名称
        String pdName = pdMap.get(fields[1]);

        // 3 拼接
        k.set(fields[0] + "\t" + pdName + "\t" + fields[2]);

        // 4 写出
        context.write(k, NullWritable.get());
    }

}
