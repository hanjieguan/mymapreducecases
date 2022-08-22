package com.mytest.mymr.g_combiner2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

//public class MyCombiner extends Reducer<Text, MyBean, Text, MyBean> {
//    @Override
//    protected void reduce(Text key, Iterable<MyBean> values, Context context) throws IOException, InterruptedException {
//        // 1 遍历所用bean，将其中的上行流量，下行流量分别累加
//        for (MyBean flowBean : values) {
//            context.write(key, flowBean);
//        }
//
//    }
//}

public class MyCombiner extends Reducer<Text, MyBean, Text, MyBean> {
    @Override
    protected void reduce(Text key, Iterable<MyBean> values, Context context) throws IOException, InterruptedException {
        long sum_upFlow=0;
        long sum_downFlow=0;
        // 2 封装对象
        MyBean resultBean = new MyBean();
        // 1 遍历所用bean，将其中的上行流量，下行流量分别累加
        for (MyBean flowBean : values) {
            sum_upFlow += flowBean.getUpFlow();
            sum_downFlow += flowBean.getDownFlow();
        }
        resultBean.setUpFlow(sum_upFlow);
        resultBean.setDownFlow(sum_downFlow);
        resultBean.setSumFlow(resultBean.cacula_sumflow());
        // 3 写出
        context.write(key, resultBean);
    }
}