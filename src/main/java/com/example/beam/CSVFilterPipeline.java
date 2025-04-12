package com.example.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Apache Beam pipeline for filtering columns from CSV files.
 * This pipeline reads a CSV file, filters specified columns,
 * and writes the result to a new CSV file.
 */
public class CSVFilterPipeline {
    private static final Logger LOG = LoggerFactory.getLogger(CSVFilterPipeline.class);

    /**
     * Pipeline options interface for CSV filter job.
     */
    public interface CSVFilterOptions extends PipelineOptions {
        @Description("Input CSV file path")
        @Default.String("input.csv")
        String getInputFile();
        void setInputFile(String value);

        @Description("Output CSV file path")
        @Default.String("output.csv")
        String getOutputFile();
        void setOutputFile(String value);

        @Description("Field indices to keep (comma-separated, 0-based)")
        @Default.String("0,1,2")
        String getFieldIndices();
        void setFieldIndices(String value);

        @Description("CSV delimiter")
        @Default.String(",")
        String getDelimiter();
        void setDelimiter(String value);

        @Description("Whether the input CSV has a header row")
        @Default.Boolean(true)
        Boolean getHasHeader();
        void setHasHeader(Boolean value);
    }

    public static void main(String[] args) {
        // Parse command line arguments
        CSVFilterOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(CSVFilterOptions.class);

        // Log configuration
        LOG.info("Starting CSV filter pipeline with configuration:");
        LOG.info("Input file: {}", options.getInputFile());
        LOG.info("Output file: {}", options.getOutputFile());
        LOG.info("Field indices: {}", options.getFieldIndices());
        LOG.info("Delimiter: {}", options.getDelimiter());
        LOG.info("Has header: {}", options.getHasHeader());

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

        // Run the pipeline
        LOG.info("Running pipeline...");
        pipeline.run().waitUntilFinish();
        LOG.info("Pipeline finished successfully.");
    }
}

