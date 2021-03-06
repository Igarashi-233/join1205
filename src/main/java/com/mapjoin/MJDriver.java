package com.mapjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.net.URI;


public class MJDriver {

        public static void main(String[] args) throws Exception {

            // 1 获取job信息

            Job job = Job.getInstance(new Configuration());

            // 2 设置加载jar包路径
            job.setJarByClass(MJMapper.class);

            // 3 关联map
            job.setMapperClass(MJMapper.class);

            // 4 设置最终输出数据类型
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);

            // 5 设置输入输出路径
            FileInputFormat.setInputPaths(job, new Path("C:\\Users\\IGARASHI\\Desktop\\MapReducer\\mapjoin\\input\\order.txt"));
            FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\IGARASHI\\Desktop\\MapReducer\\mapjoin\\output"));

            // 6 加载缓存数据
            job.addCacheFile(URI.create("file:///C:/Users/IGARASHI/Desktop/MapReducer/mapjoin/input/pd.txt"));

            // 7 Map端Join的逻辑不需要Reduce阶段，设置reduceTask数量为0
            job.setNumReduceTasks(0);

            // 8 提交
            boolean result = job.waitForCompletion(true);
            System.exit(result ? 0 : 1);
        }

}
