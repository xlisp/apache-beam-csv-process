package com.example.beam;

import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Apache Beam DoFn that filters CSV fields by indices.
 * This transformation extracts specified columns from each line of CSV input.
 */
public class FilterCSVFields extends DoFn<String, String> {
    private static final Logger LOG = LoggerFactory.getLogger(FilterCSVFields.class);
    private final List<Integer> fieldIndices;
    private final String delimiter;
    private boolean isFirstLine;
    private final boolean hasHeader;

    /**
     * Creates a new instance of the CSV field filter.
     *
     * @param fieldIndicesStr Comma-separated list of field indices to keep (0-based)
     * @param delimiter CSV field delimiter
     * @param hasHeader Whether the input has a header row
     */
    public FilterCSVFields(String fieldIndicesStr, String delimiter, boolean hasHeader) {
        this.fieldIndices = Arrays.stream(fieldIndicesStr.split(","))
                .map(String::trim)
                .map(Integer::parseInt)
                .collect(Collectors.toList());
        this.delimiter = delimiter;
        this.isFirstLine = true;
        this.hasHeader = hasHeader;
        
        LOG.info("Created FilterCSVFields with indices: {}", fieldIndices);
    }

    @ProcessElement
    public void processElement(@Element String line, OutputReceiver<String> out) {
        // Handle header row
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
                } else {
                    LOG.warn("Header index {} out of bounds (max: {})", index, header.length - 1);
                    filteredHeader.append("");
                }
            }
            String result = filteredHeader.toString();
            LOG.debug("Header transformed: {} -> {}", line, result);
            out.output(result);
            return;
        }
        
        if (isFirstLine) {
            isFirstLine = false;
        }

        // Handle data rows
        String[] fields = line.split(delimiter, -1);
        StringBuilder filteredLine = new StringBuilder();
        
        for (int i = 0; i < fieldIndices.size(); i++) {
            if (i > 0) {
                filteredLine.append(delimiter);
            }
            
            int index = fieldIndices.get(i);
            if (index < fields.length) {
                filteredLine.append(fields[index]);
            } else {
                // Handle out of bounds index
                LOG.warn("Data row index {} out of bounds (max: {})", index, fields.length - 1);
                filteredLine.append("");
            }
        }
        
        out.output(filteredLine.toString());
    }
}
