### 项目架构说明

我创建了一个完整的解决方案，包含两个项目：

1. **spark-beam-csv-processor**（原项目，已修改）
   - 核心处理逻辑，使用Apache Beam和Spark执行引擎
   - 提供了基于命令行参数的接口
   - 现在支持通过参数指定要保留的字段和CSV分隔符

2. **spark-service**（新项目）
   - 基于Spring Boot的RESTful服务
   - 提供API接口来提交CSV处理作业
   - 异步调用`SparkBeamCsvProcessor`
   - 提供作业状态跟踪和查询功能

### 如何使用

1. **构建两个项目**
   ```bash
   # 先构建处理器项目
   cd spark-beam-csv-processor
   mvn clean install
   
   # 然后构建服务项目
   cd ../spark-service
   mvn clean package
   ```

2. **启动服务**
   ```bash
   java -jar target/spark-service-1.0-SNAPSHOT.jar
   ```

3. **通过API提交作业**
   ```bash
   curl -X POST "http://localhost:8080/api/spark/process-csv?inputFile=/path/to/input.csv&outputFile=/path/to/output.csv&fields=0,2,5&delimiter=,"
   ```

4. **查询作业状态**
   ```bash
   curl -X GET "http://localhost:8080/api/spark/job-status/{jobId}"
   ```

5. **查看所有作业**
   ```bash
   curl -X GET "http://localhost:8080/api/spark/jobs"
   ```

### 注意事项

1. 两个项目需要在Maven仓库中相互关联，确保`spark-beam-csv-processor`先被构建并安装到本地Maven仓库
2. 可以根据需要调整`ExecutorService`线程池大小来控制并发作业数量
3. 在生产环境中，你可能需要添加更多的错误处理和安全验证
4. 日志记录已经添加，但在生产环境中可能需要更详细的日志配置

如果你需要更多功能，例如取消作业、设置超时、保存历史记录等，可以进一步扩展这个基础框架。

---

# 解耦方案

* spark-service/src/main/java/com/example/service/SparkServiceApplication_v2.java

两个项目完全解耦，`spark-service`不能直接导入`SparkBeamCsvProcessor`类，并且需要添加根据`format.json`文件解析表头的功能。以下是修改后的解决方案。

### 解决方案说明

#### 1. 关于项目解耦

我已经对代码进行了重构，现在：

- `spark-service`不再直接导入`SparkBeamCsvProcessor`类
- 而是通过`Runtime.exec()`来执行`spark-submit`命令
- `spark-service`只负责作业管理和文件处理，真正的CSV处理逻辑完全委托给另一个项目执行

#### 2. format.json头部信息处理

新增的功能：

- 新增了`formatJsonPath`参数，用于指定format.json文件的路径
- 解析format.json文件中的"headers"数组获取表头信息
- 在处理完成后，将表头信息添加到输出CSV文件的第一行

#### 3. 流程说明

1. 接收处理请求，生成作业ID
2. 异步执行处理，首先检查并解析format.json文件
3. 构建并执行`spark-submit`命令，将CSV处理委托给`SparkBeamCsvProcessor`
4. 处理完成后，如果有表头信息，添加到输出文件中
5. 更新作业状态

### 使用方法

1. **确保`SparkBeamCsvProcessor`已经打包成JAR文件**

2. **修改`buildSparkSubmitCommand`方法中的JAR路径**
   ```java
   cmd.append("/path/to/spark-beam-csv-processor-1.0-SNAPSHOT.jar ");
   ```

3. **启动服务**
   ```bash
   java -jar target/spark-service-1.0-SNAPSHOT.jar
   ```

4. **提交带有format.json的处理请求**
   ```bash
   curl -X POST "http://localhost:8080/api/spark/process-csv?inputFile=/path/to/input.csv&outputFile=/path/to/output.csv&fields=0,2,5&delimiter=,&formatJsonPath=/path/to/format.json"
   ```

### 注意事项

1. **路径配置**：确保正确设置`SparkBeamCsvProcessor` JAR文件的路径。
2. **Spark配置**：根据你的环境修改`spark-submit`命令中的配置参数。
3. **错误处理**：代码已添加错误处理，但在生产环境中可能需要更健壮的错误处理机制。
4. **临时文件管理**：使用临时文件确保表头正确添加，完成后删除临时文件。
5. **资源管理**：处理大文件时可能需要考虑内存和磁盘空间。

这个解决方案确保了两个项目完全解耦，同时添加了解析format.json并处理表头的功能。

---

# 使用CHOR方案

使用CHOR（可能是指协程或消息编排系统）来管理作业查询和触发。我将修改`spark-service`项目以支持CHOR消息通信模式。

### 解决方案说明

我设计了一个基于JMS的CHOR消息系统，支持作业触发和状态查询。系统包含以下主要特点：

#### 1. CHOR消息支持

- 使用ActiveMQ作为JMS消息代理
- 定义了三个关键消息队列：
  - `job-request-queue`：接收作业请求消息
  - `job-status-queue`：发送作业状态更新
  - `job-result-queue`：发送作业完成结果

#### 2. 消息类型

添加了多种CHOR消息类型支持：
- `START_JOB`：触发新的CSV处理作业
- `GET_JOB_STATUS`：查询特定作业状态
- `GET_ALL_JOBS`：获取所有作业的状态
- `JOB_STATUS`：作业状态响应
- `JOB_RESULT`：作业完成结果响应
- `ALL_JOBS_STATUS`：所有作业状态响应

#### 3. 作业流程集成

- 接收CHOR消息来启动作业
- 在作业执行的关键阶段发送状态更新
- 作业完成时发送详细结果
- 保留原有的REST API接口，实现双重接入方式

#### 4. 配置参数化

- 消息队列名称可配置
- Spark应用JAR路径可配置
- 服务器端口和其他参数可通过配置文件调整

### 使用指南

#### 设置消息队列

1. 安装并启动ActiveMQ
   ```bash
   # 下载ActiveMQ
   wget https://downloads.apache.org/activemq/5.17.2/apache-activemq-5.17.2-bin.tar.gz
   tar -xzf apache-activemq-5.17.2-bin.tar.gz
   
   # 启动ActiveMQ
   cd apache-activemq-5.17.2
   bin/activemq start
   ```

2. 修改应用配置文件
   - 更新`spring.activemq.broker-url`指向你的ActiveMQ服务器
   - 设置`sparkapp.jar.path`为你的Spark应用JAR路径

#### 发送CHOR消息

可以使用任何JMS客户端或工具发送消息，例如：

1. 使用ActiveMQ Console
2. 使用Spring JMS测试客户端
3. 使用专用的CHOR消息发送工具

消息示例：
```json
{
  "type": "START_JOB",
  "inputFile": "/data/input.csv",
  "outputFile": "/data/output.csv",
  "fields": "0,2,5",
  "delimiter": ",",
  "formatJsonPath": "/data/format.json"
}
```

#### 监听CHOR响应

设置监听器来接收以下队列的消息：
- `job-status-queue`：获取作业状态更新
- `job-result-queue`：获取作业完成结果

这种基于CHOR消息的设计允许你的系统：
1. 解耦组件之间的通信
2. 实现异步作业处理
3. 支持分布式部署
4. 提供可靠的状态跟踪和通知

如果你需要进一步的改进，可以考虑添加消息持久化、消息重试机制或更复杂的消息路由策略。

