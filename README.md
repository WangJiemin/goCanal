## 工作原理
它是 Canal 的 golang 客户端，它与 Canal 是采用的Socket来进行通信的，传输协议是TCP，交互协议采用的是 Google Protocol Buffer 3.0。

## 工作流程
* Master发生数据变更写入到binlog
* Canal 模拟 MySQL SLAVE 开启io_thread 线程 发起dump请求,获取Master binlog并且解析
* goCanal 和 Canal 进行连接消费它
* Canal发送解析后的数据给 goCanal
* 收到数据，消费成功
* Canal记录消费位置

## goCanal解析流程
![Canal](protocol)(images/proto.jpg)
```proto
数据对象格式：EntryProtocol.proto

Entry
	Header
		logfileName [binlog文件名]
		logfileOffset [binlog position]
		executeTime [binlog里记录变更发生的时间戳]
		schemaName [数据库实例]
		tableName [表名]
		eventType [insert/update/delete类型]
	entryType 	[事务头BEGIN/事务尾END/数据ROWDATA]
	storeValue 	[byte数据,可展开，对应的类型为RowChange]
RowChange
isDdl		[是否是ddl变更操作，比如create table/drop table]
sql		[具体的ddl sql]
rowDatas	[具体insert/update/delete的变更数据，可为多条，1个binlog event事件可对应多条变更，比如批处理]
beforeColumns [Column类型的数组]
afterColumns [Column类型的数组]


Column
index		[column序号]
sqlType		[jdbc type]
name		[column name]
isKey		[是否为主键]
updated		[是否发生过变更]
isNull		[值是否为null]
value		[具体的内容，注意为文本]

说明：
可以提供数据库变更前和变更后的字段内容，针对binlog中没有的name,isKey等信息进行补全
可以提供ddl的变更语句
```

## 使用帮助

## 1.安装Canal
Canal的安装以及配置使用请查看 https://github.com/alibaba/canal/wiki/QuickStart

### 2.建立一个golang  控制台项目

### 3.为该项目从 go get 安装 canal-go

````shell
go get  github.com/WangJiemin/goCanal
````

## 参考：
[CanalClient](https://github.com/CanalClient/canal-go)(感谢大神开源)

