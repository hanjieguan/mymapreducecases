package com.mytest.mymr.g_combiner2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MyDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        // 2 关联本Driver程序的jar
        job.setJarByClass(MyDriver.class);
        // 3 关联Mapper和Reducer的jar
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        // 4 设置Mapper输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MyBean.class);
        // 5 设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MyBean.class);
        // 如果不设置InputFormat，它默认用的是TextInputFormat.class
        job.setInputFormatClass(CombineTextInputFormat.class);
        //虚拟存储切片最大值设置
        CombineTextInputFormat.setMaxInputSplitSize(job, 1500);
        // 指定Combiner 如果reduce的输入输出路径一致则可以直接设置为reducer.class
//        job.setCombinerClass(MyCombiner.class);
        // 9 同时指定相应数量的reduce task
//        job.setNumReduceTasks(5);
//        job.setNumReduceTasks(0);
        // 6 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path("src/source/combiner2"));
        FileOutputFormat.setOutputPath(job, new Path("src/output/combiner2/out1"));
        // 7 提交job
        boolean result = job.waitForCompletion(true);
    }
}
