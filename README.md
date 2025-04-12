# CSV Beam Filter

一个基于Apache Beam的CSV过滤工具，用于从CSV文件中筛选特定字段并输出到新文件。

* java vs clojure: 

```java
        // Create the pipeline
        Pipeline pipeline = Pipeline.create(options);

        // Read CSV file
        PCollection<String> lines = pipeline.apply("ReadCSV", 
                TextIO.read().from(options.getInputFile()));

        // Filter fields
        PCollection<String> filteredLines = lines.apply("FilterFields", 
                ParDo.of(new FilterCSVFields(
                        options.getFieldIndices(), 
                        options.getDelimiter(), 
                        options.getHasHeader())));

        // Write filtered CSV
        filteredLines.apply("WriteCSV", 
                TextIO.write().to(options.getOutputFile()).withoutSharding());
```

```clj
    (-> pipeline
        (.apply "ReadLines" (TextIO/read (io/file input-file)))
        (.apply "ParseCSV" (ParDo/of (create-csv-parser-fn field-indices has-header)))
        (.setCoder (StringUtf8Coder/of))
        (.apply "WriteCSV" (TextIO/write (io/file output-file))))
```

## 功能

- 从输入CSV文件读取数据
- 根据指定的字段索引筛选出需要的列
- 支持自定义分隔符
- 支持有无标题行的CSV文件
- 将筛选后的数据写入到输出CSV文件

## 构建项目

使用Maven构建项目：

```bash
mvn clean package
```

这将在`target`目录下生成两个JAR文件：
- `csv-beam-filter-1.0-SNAPSHOT.jar` - 仅包含项目代码的JAR
- `csv-beam-filter-bundled-1.0-SNAPSHOT.jar` - 包含所有依赖的可执行JAR

## 使用方法

使用以下命令运行程序：

```bash
java -jar target/csv-beam-filter-bundled-1.0-SNAPSHOT.jar \
  --inputFile=<输入CSV文件路径> \
  --outputFile=<输出CSV文件路径> \
  --fieldIndices=<要保留的字段索引，以逗号分隔> \
  --delimiter=<CSV分隔符> \
  --hasHeader=<是否有标题行>
```

### 参数说明

- `--inputFile`: 输入CSV文件路径（默认：input.csv）
- `--outputFile`: 输出CSV文件路径（默认：output.csv）
- `--fieldIndices`: 要保留的字段索引，从0开始，以逗号分隔（默认：0,1,2）
- `--delimiter`: CSV字段分隔符（默认：,）
- `--hasHeader`: 输入CSV是否包含标题行（默认：true）

### 示例

从`input.csv`中筛选第1、第3和第5列（索引从0开始），输出到`output.csv`：

```bash
java -jar target/csv-beam-filter-bundled-1.0-SNAPSHOT.jar \
  --inputFile=input.csv \
  --outputFile=output.csv \
  --fieldIndices=0,2,4
```

处理制表符分隔的CSV文件：

```bash
java -jar target/csv-beam-filter-bundled-1.0-SNAPSHOT.jar \
  --inputFile=input.tsv \
  --outputFile=output.tsv \
  --fieldIndices=0,1,2 \
  --delimiter="\t"
```

处理没有标题行的CSV文件：

```bash
java -jar target/csv-beam-filter-bundled-1.0-SNAPSHOT.jar \
  --inputFile=input.csv \
  --outputFile=output.csv \
  --fieldIndices=0,1,2 \
  --hasHeader=false
```

## 使用其他执行引擎

默认情况下，程序使用Apache Beam的DirectRunner在本地执行。如果需要使用其他执行引擎（如Spark、Flink或Google Cloud Dataflow），可以在pom.xml中添加相应的依赖，并在运行时指定相应的runner。

例如，使用Spark Runner：

```bash
java -jar target/csv-beam-filter-bundled-1.0-SNAPSHOT.jar \
  --inputFile=input.csv \
  --outputFile=output.csv \
  --fieldIndices=0,1,2 \
  --runner=SparkRunner
```
