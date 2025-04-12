package com.example.beam;

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class CSVFilterPipelineTest {

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testFilterCSVFieldsWithHeader() {
        // Sample data
        List<String> inputLines = Arrays.asList(
                "id,name,age,city,state",
                "1,John,30,New York,NY",
                "2,Alice,25,San Francisco,CA",
                "3,Bob,35,Seattle,WA"
        );

        // Create input PCollection
        PCollection<String> input = pipeline.apply(Create.of(inputLines).withCoder(StringUtf8Coder.of()));

        // Apply transformation (keeping id, name, and city columns: 0, 1, 3)
        PCollection<String> output = input.apply(ParDo.of(new FilterCSVFields("0,1,3", ",", true)));

        // Expected output
        List<String> expectedOutput = Arrays.asList(
                "id,name,city",
                "1,John,New York",
                "2,Alice,San Francisco",
                "3,Bob,Seattle"
        );

        // Assert
        PAssert.that(output).containsInAnyOrder(expectedOutput);

        // Run the pipeline
        pipeline.run();
    }

    @Test
    public void testFilterCSVFieldsWithoutHeader() {
        // Sample data without header
        List<String> inputLines = Arrays.asList(
                "1,John,30,New York,NY",
                "2,Alice,25,San Francisco,CA",
                "3,Bob,35,Seattle,WA"
        );

        // Create input PCollection
        PCollection<String> input = pipeline.apply(Create.of(inputLines).withCoder(StringUtf8Coder.of()));

        // Apply transformation (keeping id, name, and city columns: 0, 1, 3)
        PCollection<String> output = input.apply(ParDo.of(new FilterCSVFields("0,1,3", ",", false)));

        // Expected output
        List<String> expectedOutput = Arrays.asList(
                "1,John,New York",
                "2,Alice,San Francisco",
                "3,Bob,Seattle"
        );

        // Assert
        PAssert.that(output).containsInAnyOrder(expectedOutput);

        // Run the pipeline
        pipeline.run();
    }

    @Test
    public void testFilterCSVFieldsWithCustomDelimiter() {
        // Sample data with tab delimiter
        List<String> inputLines = Arrays.asList(
                "id\tname\tage\tcity\tstate",
                "1\tJohn\t30\tNew York\tNY",
                "2\tAlice\t25\tSan Francisco\tCA",
                "3\tBob\t35\tSeattle\tWA"
        );

        // Create input PCollection
        PCollection<String> input = pipeline.apply(Create.of(inputLines).withCoder(StringUtf8Coder.of()));

        // Apply transformation (keeping id, name, and state columns: 0, 1, 4)
        PCollection<String> output = input.apply(ParDo.of(new FilterCSVFields("0,1,4", "\t", true)));

        // Expected output
        List<String> expectedOutput = Arrays.asList(
                "id\tname\tstate",
                "1\tJohn\tNY",
                "2\tAlice\tCA",
                "3\tBob\tWA"
        );

        // Assert
        PAssert.that(output).containsInAnyOrder(expectedOutput);

        // Run the pipeline
        pipeline.run();
    }
}
