package com.reducejoin;

import com.bean.OrderBean;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class RJReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {

    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        //迭代器
        Iterator<NullWritable> iterator = values.iterator();
        //获取第一个OrderBean
        iterator.next();
        //从第一个OrderBean种取出品牌名
        String pname = key.getPname();
        //遍历剩余OrderBean, 设置品牌名并写出
        while (iterator.hasNext()){
            iterator.next();
            key.setPname(pname);
            context.write(key, NullWritable.get());
        }
    }
}
