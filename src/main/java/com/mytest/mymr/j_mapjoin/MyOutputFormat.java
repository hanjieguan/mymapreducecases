package com.mytest.mymr.j_mapjoin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MyOutputFormat extends FileOutputFormat<MyKeyBean, MyBean> {
    @Override
    public RecordWriter<MyKeyBean, MyBean> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        MyRecordWritert myRecordWritert = new MyRecordWritert(job);
        return myRecordWritert;
    }


//    @Override
//    public RecordWriter<Text, MyBean> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
//        MyRecordWritert myRecordWritert = new MyRecordWritert(job);
//        return myRecordWritert;
//    }
}
