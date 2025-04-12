import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class CSVFilterPipeline {

    // 定义程序参数接口
    public interface CSVFilterOptions extends PipelineOptions {
        @Description("输入CSV文件路径")
        @Default.String("input.csv")
        String getInputFile();
        void setInputFile(String value);

        @Description("输出CSV文件路径")
        @Default.String("output.csv")
        String getOutputFile();
        void setOutputFile(String value);

        @Description("要保留的字段索引，以逗号分隔")
        @Default.String("0,1,2")
        String getFieldIndices();
        void setFieldIndices(String value);

        @Description("CSV分隔符")
        @Default.String(",")
        String getDelimiter();
        void setDelimiter(String value);

        @Description("是否包含标题行")
        @Default.Boolean(true)
        Boolean getHasHeader();
        void setHasHeader(Boolean value);
    }

    // CSV处理转换器
    static class FilterCSVFields extends DoFn<String, String> {
        private final List<Integer> fieldIndices;
        private final String delimiter;
        private boolean isFirstLine;
        private final boolean hasHeader;

        public FilterCSVFields(String fieldIndicesStr, String delimiter, boolean hasHeader) {
            this.fieldIndices = Arrays.stream(fieldIndicesStr.split(","))
                    .map(String::trim)
                    .map(Integer::parseInt)
                    .collect(Collectors.toList());
            this.delimiter = delimiter;
            this.isFirstLine = true;
            this.hasHeader = hasHeader;
        }

        @ProcessElement
        public void processElement(@Element String line, OutputReceiver<String> out) {
            // 处理标题行
            if (isFirstLine && hasHeader) {
                isFirstLine = false;
                String[] header = line.split(delimiter, -1);
                StringBuilder filteredHeader = new StringBuilder();
                
                for (int i = 0; i < fieldIndices.size(); i++) {
                    if (i > 0) {
                        filteredHeader.append(delimiter);
                    }
                    int index = fieldIndices.get(i);
                    if (index < header.length) {
                        filteredHeader.append(header[index]);
                    }
                }
                out.output(filteredHeader.toString());
                return;
            }
            
            if (isFirstLine) {
                isFirstLine = false;
            }

            // 处理数据行
            String[] fields = line.split(delimiter, -1);
            StringBuilder filteredLine = new StringBuilder();
            
            for (int i = 0; i < fieldIndices.size(); i++) {
                if (i > 0) {
                    filteredLine.append(delimiter);
                }
                int index = fieldIndices.get(i);
                if (index < fields.length) {
                    filteredLine.append(fields[index]);
                }
            }
            out.output(filteredLine.toString());
        }
    }

    public static void main(String[] args) {
        // 解析命令行参数
        CSVFilterOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(CSVFilterOptions.class);

        // 创建Pipeline
        Pipeline pipeline = Pipeline.create(options);

        // 读取CSV文件
        PCollection<String> lines = pipeline.apply("ReadCSV", TextIO.read().from(options.getInputFile()));

        // 筛选字段
        PCollection<String> filteredLines = lines.apply("FilterFields", ParDo.of(
                new FilterCSVFields(options.getFieldIndices(), options.getDelimiter(), options.getHasHeader())
        ));

        // 写入结果到新CSV文件
        filteredLines.apply("WriteCSV", TextIO.write().to(options.getOutputFile()).withoutSharding());

        // 运行Pipeline
        pipeline.run().waitUntilFinish();
    }
}
