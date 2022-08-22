package com.mytest.mymr.k_mapjoin2;

import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartition extends Partitioner<MyKeyBean, MyBean> {
    @Override
    public int getPartition(MyKeyBean myKeyBean, MyBean myBean, int numPartitions) {
        String key = myKeyBean.getPhoneNum();
        // 1 获取电话号码的前三位
        String preNum = key.substring(0, 3);
        int partition = 4;
        // 判断
        if ("136".equals(preNum)) {
            partition = 0;
        }else if ("137".equals(preNum)) {
            partition = 1;
        }else if ("138".equals(preNum)) {
            partition = 2;
        }else if ("139".equals(preNum)) {
            partition = 3;
        }
        return partition;
    }
}
