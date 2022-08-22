### 总结一
重写OutputFormat，但是OutputFormat是抽象的，没有具体的实现，

FileoutputFormat也是抽象的，但有具体的实现，因此可以继承FileoutputFormat,

（类似于TextOutputFormat）

继承后重写getRecordWriter方法，返回一个自定义的RecordWriter

### 总结二
若设置多个RT，在重写output时，多个RT无法写入同一个output中，只有同一个RT才能写进一个output里

### 总结三
若reduce数量大于分组数量，则会产生空白的结果文件

若reduce数量小于分组数量，则会报错