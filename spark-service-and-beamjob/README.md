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

