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

