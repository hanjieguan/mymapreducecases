package com.mytest.mymr.g_combiner2;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class MyMapper extends Mapper<LongWritable, Text, Text, MyBean> {
    MyBean v = new MyBean();
    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1 获取一行
        String line = value.toString();
        // 2 切割字段
        String[] fields = line.split(",");
        // 3 封装对象
        // 取出手机号码
        String phoneNum = fields[1];
        // 取出上行流量和下行流量
        long upFlow = Long.parseLong(fields[4]);
        long downFlow = Long.parseLong(fields[5]);
        k.set(phoneNum);
        v.setUpFlow(upFlow);
        v.setDownFlow(downFlow);
        v.setSumFlow(v.cacula_sumflow());
        // 4 写出
        context.write(k, v);
    }
}
