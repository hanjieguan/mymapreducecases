package com.mytest.mymr.l_compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class MyDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Configuration configuration = new Configuration();
        configuration.setBoolean("mapreduce.map.output.compress",true);
        configuration.setClass("mapreduce.map.output.compress.codec", BZip2Codec.class, CompressionCodec.class);
        Job job = Job.getInstance(configuration);
        // 2 关联本Driver程序的jar
        job.setJarByClass(MyDriver.class);
        // 3 关联Mapper和Reducer的jar
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        // 4 设置Mapper输出的kv类型
        job.setMapOutputKeyClass(MyKeyBean.class);
        job.setMapOutputValueClass(MyBean.class);
        // 5 设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MyBean.class);
        // 8 指定自定义数据分区
        job.setPartitionerClass(MyPartition.class);
        // 指定Combiner 如果reduce的输入输出路径一致则可以直接设置为reducer.class
        job.setCombinerClass( MyCombiner.class);
//        // 如果不设置InputFormat，它默认用的是TextInputFormat.class
//        job.setInputFormatClass(CombineTextInputFormat.class);
//        //虚拟存储切片最大值设置
//        CombineTextInputFormat.setMaxInputSplitSize(job, 10000000);
//         // 9 同时指定相应数量的reduce task
//        job.setNumReduceTasks(5); // 此时无法设置RT
        //设置自定义的outputformat
        job.setOutputFormatClass(MyOutputFormat.class);
        // 加入缓存
        job.addCacheFile(new URI("src/source/compress/map_join_total.txt"));
        // 6 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path("src/source/compress/datas"));
        FileOutputFormat.setOutputPath(job, new Path("src/output/compress/out1"));
        // 7 提交job
        boolean result = job.waitForCompletion(true);
    }
}
