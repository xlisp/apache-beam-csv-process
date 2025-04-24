package com.example.service;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.*;
import org.springframework.http.ResponseEntity;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.example.beam.SparkBeamCsvProcessor;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SpringBootApplication
public class SparkServiceApplication {

    private static final Logger logger = LoggerFactory.getLogger(SparkServiceApplication.class);
    
    public static void main(String[] args) {
        SpringApplication.run(SparkServiceApplication.class, args);
    }

    @RestController
    @RequestMapping("/api/spark")
    public static class SparkController {
        
        private final ExecutorService executorService = Executors.newFixedThreadPool(5);
        private final Map<String, JobStatus> jobStatusMap = new HashMap<>();
        
        @PostMapping("/process-csv")
        public ResponseEntity<Map<String, Object>> processCsv(
                @RequestParam String inputFile,
                @RequestParam String outputFile,
                @RequestParam(required = false, defaultValue = "0,1,2") String fields,
                @RequestParam(required = false, defaultValue = ",") String delimiter) {
            
            // 生成作业ID
            String jobId = "job-" + System.currentTimeMillis();
            
            // 解析要保留的字段
            String[] fieldIndexes = fields.split(",");
            Integer[] fieldsToKeep = new Integer[fieldIndexes.length];
            for (int i = 0; i < fieldIndexes.length; i++) {
                fieldsToKeep[i] = Integer.parseInt(fieldIndexes[i].trim());
            }
            
            // 记录作业状态
            JobStatus status = new JobStatus(jobId, "SUBMITTED", inputFile, outputFile);
            jobStatusMap.put(jobId, status);
            
            // 异步执行Spark作业
            CompletableFuture.runAsync(() -> {
                try {
                    logger.info("Starting CSV processing job: {}", jobId);
                    jobStatusMap.get(jobId).setStatus("RUNNING");
                    
                    // 调用SparkBeamCsvProcessor
                    String[] sparkArgs = new String[]{
                            "--inputFile=" + inputFile,
                            "--outputFile=" + outputFile,
                            "--fields=" + fields,
                            "--delimiter=" + delimiter
                    };
                    
                    SparkBeamCsvProcessor.main(sparkArgs);
                    
                    jobStatusMap.get(jobId).setStatus("COMPLETED");
                    logger.info("Job completed successfully: {}", jobId);
                } catch (Exception e) {
                    jobStatusMap.get(jobId).setStatus("FAILED");
                    jobStatusMap.get(jobId).setErrorMessage(e.getMessage());
                    logger.error("Job failed: " + jobId, e);
                }
            }, executorService);
            
            // 返回作业ID和状态
            Map<String, Object> response = new HashMap<>();
            response.put("jobId", jobId);
            response.put("status", "SUBMITTED");
            response.put("message", "CSV processing job submitted successfully");
            
            return ResponseEntity.ok(response);
        }
        
        @GetMapping("/job-status/{jobId}")
        public ResponseEntity<JobStatus> getJobStatus(@PathVariable String jobId) {
            JobStatus status = jobStatusMap.get(jobId);
            if (status == null) {
                return ResponseEntity.notFound().build();
            }
            return ResponseEntity.ok(status);
        }
        
        @GetMapping("/jobs")
        public ResponseEntity<Map<String, JobStatus>> getAllJobs() {
            return ResponseEntity.ok(jobStatusMap);
        }
    }
    
    public static class JobStatus {
        private String jobId;
        private String status;
        private String inputFile;
        private String outputFile;
        private String errorMessage;
        private long startTime;
        
        public JobStatus(String jobId, String status, String inputFile, String outputFile) {
            this.jobId = jobId;
            this.status = status;
            this.inputFile = inputFile;
            this.outputFile = outputFile;
            this.startTime = System.currentTimeMillis();
        }
        
        // Getters and setters
        public String getJobId() { return jobId; }
        public void setJobId(String jobId) { this.jobId = jobId; }
        
        public String getStatus() { return status; }
        public void setStatus(String status) { this.status = status; }
        
        public String getInputFile() { return inputFile; }
        public void setInputFile(String inputFile) { this.inputFile = inputFile; }
        
        public String getOutputFile() { return outputFile; }
        public void setOutputFile(String outputFile) { this.outputFile = outputFile; }
        
        public String getErrorMessage() { return errorMessage; }
        public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
        
        public long getStartTime() { return startTime; }
        public void setStartTime(long startTime) { this.startTime = startTime; }
        
        public long getDuration() {
            return System.currentTimeMillis() - startTime;
        }
    }
}
