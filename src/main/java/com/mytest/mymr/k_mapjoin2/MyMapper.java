package com.mytest.mymr.k_mapjoin2;


import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;


public class MyMapper extends Mapper<LongWritable, Text, MyKeyBean, MyBean> {
    MyBean v = new MyBean();
    MyKeyBean k = new MyKeyBean();
    String filename;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        InputSplit inputSplit = context.getInputSplit();
        FileSplit fileSplit = (FileSplit) inputSplit;
        filename = fileSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (filename.contains("detail")) {
            // 1 获取一行
            String line = value.toString();
            // 2 切割字段
            String[] fields = line.split(",");
            String phoneNum = fields[1];
            // 取出上行流量和下行流量
            long upFlow = Long.parseLong(fields[4]);
            long downFlow = Long.parseLong(fields[5]);
            k.setPhoneNum(phoneNum);
            k.setIp(fields[2]);
            v.setUpFlow(upFlow);
            v.setDownFlow(downFlow);
            v.setSumFlow(v.cacula_sumflow());
            v.setName("");
            v.setFlag("detail");
        }
        // 4 写出
        context.write(k, v);
    }
}
