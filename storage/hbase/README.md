Hbase client简单封装，实现的是hbase 0.9支持的thrift接口
阿里云的hbase标准版使用的是0.9协议，增强版需要hbase 2.0协议

生成代码
```
thrift --gen go Hbase.thrift
```

生成后有一些是不兼容的内容需要手动替换，Text []byte 被复制给string定义的字段
