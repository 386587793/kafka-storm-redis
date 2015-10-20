# common 包
## config
操作 storm的配置文件。
从指定的配置文件读取提交topo时的配置信息。
可以从本地文件系统,现在还支持从hdfs读取配置文件。只需要配置读取的文件系统为hdfs。
## db/redis
操作redis。
有单例实现redis cluster的操作。
另外修改了一些storm-redis的源码，剔除了JedisPool,只是支持了redis cluster。
把JedisClusterConfig修改成从storm config文件中读取配置，简化了操作。在AbstractRedisBolt.java中实现。
bolt只需要继承AbstractRedisBolt.java就可实现redis操作。
## fs
文件系统操作。
实现从本地或hdfs的文件流读取。
## util
一些工具类/方法。