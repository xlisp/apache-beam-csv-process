

```sh
mvn clean package

spark-submit --class com.example.beam.SparkBeamCsvProcessor target/spark-beam-csv-processor-1.0-SNAPSHOT.jar --inputFile=path/to/input.csv --outputFile=path/to/output.csv

```

---

使用Spark作为执行引擎，并通过Apache Beam进行批处理，实现CSV文件的字段过滤功能。

```java
package com.example.beam;

import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.util.Arrays;
import java.util.List;

public class SparkBeamCsvProcessor {

    // 自定义DoFn类来处理CSV行并过滤指定字段
    static class FilterCsvFields extends DoFn<String, String> {
        private final List<Integer> fieldsToKeep;
        private final String delimiter;
        
        public FilterCsvFields(List<Integer> fieldsToKeep, String delimiter) {
            this.fieldsToKeep = fieldsToKeep;
            this.delimiter = delimiter;
        }
        
        @ProcessElement
        public void processElement(@Element String line, OutputReceiver<String> out) {
            // 跳过空行
            if (line == null || line.trim().isEmpty()) {
                return;
            }
            
            String[] fields = line.split(delimiter);
            StringBuilder result = new StringBuilder();
            
            boolean isFirst = true;
            for (int fieldIndex : fieldsToKeep) {
                if (fieldIndex < fields.length) {
                    if (!isFirst) {
                        result.append(delimiter);
                    }
                    result.append(fields[fieldIndex]);
                    isFirst = false;
                }
            }
            
            out.output(result.toString());
        }
    }
    
    public static void main(String[] args) {
        // 创建Pipeline选项
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        
        // 设置使用Spark Runner
        options.setRunner(SparkRunner.class);
        
        // 创建Pipeline
        Pipeline pipeline = Pipeline.create(options);
        
        // 输入和输出文件路径 - 这里应从命令行参数或配置中获取
        String inputFile = "input.csv";
        String outputFile = "output.csv";
        
        // 要保留的字段索引（从0开始）
        List<Integer> fieldsToKeep = Arrays.asList(0, 2, 5); // 保留第1, 3, 6列
        
        // CSV分隔符
        String delimiter = ",";
        
        // 读取CSV文件
        PCollection<String> lines = pipeline.apply("ReadCSV", TextIO.read().from(inputFile));
        
        // 过滤字段
        PCollection<String> filteredLines = lines.apply("FilterFields", 
                ParDo.of(new FilterCsvFields(fieldsToKeep, delimiter)));
        
        // 写入结果到新的CSV文件
        filteredLines.apply("WriteCSV", TextIO.write().to(outputFile)
                .withoutSharding()
                .withCoder(StringUtf8Coder.of()));
        
        // 执行Pipeline
        pipeline.run().waitUntilFinish();
        
        System.out.println("CSV处理完成！");
    }
}

```

你还需要设置相应的Maven依赖项，下面是一个适用的`pom.xml`文件：

```xml
<project xmlns="http://maven.apache.org/POM/4.0.0"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.example</groupId>
    <artifactId>spark-beam-csv-processor</artifactId>
    <version>1.0-SNAPSHOT</version>

    <properties>
        <maven.compiler.source>1.8</maven.compiler.source>
        <maven.compiler.target>1.8</maven.compiler.target>
        <beam.version>2.46.0</beam.version>
        <spark.version>3.3.2</spark.version>
    </properties>

    <dependencies>
        <!-- Apache Beam Core -->
        <dependency>
            <groupId>org.apache.beam</groupId>
            <artifactId>beam-sdks-java-core</artifactId>
            <version>${beam.version}</version>
        </dependency>

        <!-- Beam Spark Runner -->
        <dependency>
            <groupId>org.apache.beam</groupId>
            <artifactId>beam-runners-spark</artifactId>
            <version>${beam.version}</version>
        </dependency>

        <!-- Spark Core -->
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-core_2.12</artifactId>
            <version>${spark.version}</version>
        </dependency>

        <!-- Spark SQL for CSV handling -->
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql_2.12</artifactId>
            <version>${spark.version}</version>
        </dependency>

        <!-- Apache Commons CSV -->
        <dependency>
            <groupId>org.apache.commons</groupId>
            <artifactId>commons-csv</artifactId>
            <version>1.9.0</version>
        </dependency>

        <!-- SLF4J API -->
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-api</artifactId>
            <version>1.7.32</version>
        </dependency>

        <!-- SLF4J Simple binding -->
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-simple</artifactId>
            <version>1.7.32</version>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>3.8.1</version>
                <configuration>
                    <source>${maven.compiler.source}</source>
                    <target>${maven.compiler.target}</target>
                </configuration>
            </plugin>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-shade-plugin</artifactId>
                <version>3.2.4</version>
                <executions>
                    <execution>
                        <phase>package</phase>
                        <goals>
                            <goal>shade</goal>
                        </goals>
                        <configuration>
                            <transformers>
                                <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                                    <mainClass>com.example.beam.SparkBeamCsvProcessor</mainClass>
                                </transformer>
                                <transformer implementation="org.apache.maven.plugins.shade.resource.ServicesResourceTransformer"/>
                            </transformers>
                            <filters>
                                <filter>
                                    <artifact>*:*</artifact>
                                    <excludes>
                                        <exclude>META-INF/*.SF</exclude>
                                        <exclude>META-INF/*.DSA</exclude>
                                        <exclude>META-INF/*.RSA</exclude>
                                    </excludes>
                                </filter>
                            </filters>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
</project>

```

### 使用说明

1. 代码实现了一个基于Apache Beam的CSV处理器，使用Spark作为执行引擎
2. 功能包括：
   - 读取CSV文件
   - 过滤指定列（按索引选择需要保留的字段）
   - 输出新的CSV文件

3. 代码中的关键部分：
   - `FilterCsvFields` 类：一个自定义的DoFn，处理每一行CSV数据并只保留指定列
   - `SparkRunner`：作为Apache Beam的执行引擎
   - 通过`fieldsToKeep`列表控制需要保留的列索引

4. 如何自定义：
   - 修改`inputFile`和`outputFile`路径
   - 调整`fieldsToKeep`列表中的索引值来选择你想要保留的字段
   - 如果需要，可以修改`delimiter`来适应不同的CSV分隔符

5. 运行方法：
   ```bash
   mvn clean package
   spark-submit --class com.example.beam.SparkBeamCsvProcessor target/spark-beam-csv-processor-1.0-SNAPSHOT.jar --inputFile=path/to/input.csv --outputFile=path/to/output.csv
   ```

如果你有其他需求或需要更复杂的CSV处理逻辑，可以进一步扩展这个基础框架。

