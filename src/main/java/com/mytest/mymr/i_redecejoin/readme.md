### 报错一
```shell script
java.lang.Exception: java.lang.ClassCastException: org.apache.hadoop.mapreduce.lib.input.CombineFileSplit cannot be cast to org.apache.hadoop.mapreduce.lib.input.FileSplit
	at org.apache.hadoop.mapred.LocalJobRunner$Job.runTasks(LocalJobRunner.java:492) ~[hadoop-mapreduce-client-common-3.1.3.jar:?]
	at org.apache.hadoop.mapred.LocalJobRunner$Job.run(LocalJobRunner.java:552) [hadoop-mapreduce-client-common-3.1.3.jar:?]
Caused by: java.lang.ClassCastException: org.apache.hadoop.mapreduce.lib.input.CombineFileSplit cannot be cast to org.apache.hadoop.mapreduce.lib.input.FileSplit
```

### 报错二
```shell script
java.lang.NullPointerException: null
	at com.mytest.mymr.i_redecejoin.MyCombiner.reduce(MyCombiner.java:28) ~[classes/:?]
	at com.mytest.mymr.i_redecejoin.MyCombiner.reduce(MyCombiner.java:7) ~[classes/:?]
```

### 报错三
```shell script
if ("136".equals(preNum) || "137".equals(preNum)||"138".equals(preNum)|| "139".equals(preNum)) {
            nomalOut.write(line.getBytes(),0,line.getBytes().length);
        }else {
            otherOut.write(line.getBytes(),0,line.getBytes().length);
        }
```
否则有乱码

查看writeUTF源码：
```shell script
* Writes two bytes of length information
     * to the output stream, followed
     * by the
     * <a href="DataInput.html#modified-utf-8">modified UTF-8</a>
```