package com.mytest.mymr.k_mapjoin2;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

class MyRecordWritert extends RecordWriter<Text, MyBean> {
    private FSDataOutputStream nomalOut;
    private FSDataOutputStream otherOut;

    public MyRecordWritert(TaskAttemptContext job) throws IOException {
        FileSystem fs = FileSystem.get(job.getConfiguration());
        nomalOut = fs.create(new Path("src/output/map_join2/out2/nomal.txt"));
        otherOut = fs.create(new Path("src/output/map_join2/out2/orther.txt"));
    }

    @Override
    public void write(Text key, MyBean value) throws IOException, InterruptedException {
        String preNum = key.toString().substring(0, 3);
        String line = key +" "+value+"\n";
        if ("136".equals(preNum) || "137".equals(preNum)||"138".equals(preNum)|| "139".equals(preNum)) {
            nomalOut.write(line.getBytes(),0,line.getBytes().length);
        }else {
            otherOut.write(line.getBytes(),0,line.getBytes().length);
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(nomalOut);
        IOUtils.closeStream(otherOut);
    }
}
