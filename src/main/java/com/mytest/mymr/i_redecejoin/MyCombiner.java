package com.mytest.mymr.i_redecejoin;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyCombiner extends Reducer<MyKeyBean, MyBean, MyKeyBean, MyBean> {
    @Override
    protected void reduce(MyKeyBean key, Iterable<MyBean> values, Context context) throws IOException, InterruptedException {
        long sum_upFlow=0;
        long sum_downFlow=0;
        String name = "";
        // 2 封装对象
        MyBean resultBean = new MyBean();
        // 1 遍历所用bean，将其中的上行流量，下行流量分别累加
        for (MyBean flowBean : values) {
            if (!flowBean.getFlag().equals("detail")) {
                name = flowBean.getName();
            }
            sum_upFlow += flowBean.getUpFlow();
            sum_downFlow += flowBean.getDownFlow();
        }
        resultBean.setUpFlow(sum_upFlow);
        resultBean.setDownFlow(sum_downFlow);
        resultBean.setSumFlow(resultBean.cacula_sumflow());
        if (!name.equals("")) {
            resultBean.setName(name);
        }else {
            resultBean.setName("");
        }
        resultBean.setFlag("");
        // 3 写出
        context.write(key, resultBean);
    }
}
