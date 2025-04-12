# CSV Beam Processor

这是一个使用 Apache Beam 和 Clojure 开发的 CSV 处理工具，用于从 CSV 文件中筛选特定字段并输出到新文件。

## 功能

- 从输入 CSV 文件中读取数据
- 根据指定索引筛选字段
- 输出筛选后的数据到新的 CSV 文件
- 支持保留或忽略头行
- 提供命令行选项自定义处理参数

## 使用方法

### 命令行选项

- `--inputFile`: 输入 CSV 文件路径 (默认: "input.csv")
- `--outputFile`: 输出 CSV 文件路径 (默认: "output.csv")
- `--fieldIndices`: 要筛选的字段索引，逗号分隔 (默认: "0,2,5")
- `--hasHeader`: CSV 是否有头行 (默认: true)

### 运行示例

```bash
# 使用默认选项运行
lein run

# 自定义选项运行
lein run --inputFile=data.csv --outputFile=filtered.csv --fieldIndices=1,3,4 --hasHeader=true

# 打包成 Uber JAR 后运行
lein uberjar
java -jar target/uberjar/csv-beam-processor-0.1.0-SNAPSHOT-standalone.jar --inputFile=data.csv --outputFile=filtered.csv
```

## 许可证

Copyright © 2025 

本项目使用 EPL 2.0 许可证，详见 LICENSE 文件。

;; resources/sample.csv
id,name,age,city,gender,salary,department
1,Zhang San,30,Beijing,M,10000,IT
2,Li Si,25,Shanghai,F,8000,HR
3,Wang Wu,35,Guangzhou,M,12000,Finance
4,Zhao Liu,28,Shenzhen,F,9000,Marketing
5,Qian Qi,32,Hangzhou,M,11000,IT

