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
