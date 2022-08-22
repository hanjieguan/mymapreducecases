package com.mytest.mymr.k_mapjoin2;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MyKeyBean implements WritableComparable<MyKeyBean> {
    private String phoneNum;
    private String ip;
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(phoneNum);
        out.writeUTF(ip);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.phoneNum=in.readUTF();
        this.ip=in.readUTF();
    }

    @Override
    public int compareTo(MyKeyBean o) {
        return o.phoneNum.hashCode()-this.phoneNum.hashCode();
    }

    @Override
    public String toString() {
        return phoneNum;
    }

    public String getPhoneNum() {
        return phoneNum;
    }

    public void setPhoneNum(String phoneNum) {
        this.phoneNum = phoneNum;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }
}
