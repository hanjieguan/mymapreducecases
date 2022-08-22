package com.mytest.mymr.k_mapjoin2;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class MyReducer extends Reducer<MyKeyBean, MyBean, Text, MyBean> {
    Map<String,String> pdMap =new HashMap<String,String>();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 读取缓存文件
        URI[] cacheFiles = context.getCacheFiles();
        Path path = new Path(cacheFiles[0]);
        FileSystem fs = FileSystem.get(context.getConfiguration());
        FSDataInputStream fis = fs.open(path);
        //通过包装流转换为reader,方便按行读取
        BufferedReader reader = new BufferedReader(new InputStreamReader(fis, "UTF-8"));
        String aline;
        while (StringUtils.isNotEmpty(aline = reader.readLine())) {
            String[] split = aline.split(",");
            pdMap.put(split[0],split[2]);
        }
        //关流
        IOUtils.closeStream(reader);
    }

    @Override
    protected void reduce(MyKeyBean key, Iterable<MyBean> values, Context context) throws IOException, InterruptedException {
        long sum_upFlow=0;
        long sum_downFlow=0;
        // 2 封装对象
        MyBean resultBean = new MyBean();
        Text resultKey = new Text();
        String phoneNum = key.getPhoneNum();
        // 1 遍历所用bean，将其中的上行流量，下行流量分别累加
        for (MyBean flowBean : values) {
            sum_upFlow += flowBean.getUpFlow();
            sum_downFlow += flowBean.getDownFlow();
        }
        resultBean.setUpFlow(sum_upFlow);
        resultBean.setDownFlow(sum_downFlow);
        resultBean.setSumFlow(resultBean.cacula_sumflow());
        resultBean.setName(pdMap.get(phoneNum));
        resultKey.set(phoneNum);
        // 3 写出
        context.write(resultKey, resultBean);
    }
}
