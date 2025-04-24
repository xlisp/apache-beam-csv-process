package com.example.beam;

import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class SparkBeamCsvProcessor {

    // 定义Pipeline选项接口
    public interface CsvProcessingOptions extends PipelineOptions {
        @Description("Path to the input CSV file")
        String getInputFile();
        void setInputFile(String value);

        @Description("Path to the output CSV file")
        String getOutputFile();
        void setOutputFile(String value);

        @Description("Comma-separated list of field indices to keep (0-based)")
        @Default.String("0,1,2")
        String getFields();
        void setFields(String value);

        @Description("CSV delimiter")
        @Default.String(",")
        String getDelimiter();
        void setDelimiter(String value);
    }

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
            
            String[] fields = line.split(delimiter, -1); // -1 to keep empty fields
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
        // 注册和解析参数
        CsvProcessingOptions options = PipelineOptionsFactory
            .fromArgs(args)
            .withValidation()
            .as(CsvProcessingOptions.class);
        
        // 设置使用Spark Runner
        options.setRunner(SparkRunner.class);
        
        // 获取参数
        String inputFile = options.getInputFile();
        String outputFile = options.getOutputFile();
        String fieldsStr = options.getFields();
        String delimiter = options.getDelimiter();
        
        // 解析要保留的字段索引
        List<Integer> fieldsToKeep = Arrays.stream(fieldsStr.split(","))
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .map(Integer::parseInt)
            .collect(Collectors.toList());
        
        System.out.println("Processing CSV file:");
        System.out.println("Input: " + inputFile);
        System.out.println("Output: " + outputFile);
        System.out.println("Fields to keep: " + fieldsToKeep);
        System.out.println("Delimiter: " + delimiter);
        
        // 创建Pipeline
        Pipeline pipeline = Pipeline.create(options);
        
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

