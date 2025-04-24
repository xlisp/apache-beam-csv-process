package com.example.service;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.*;
import org.springframework.http.ResponseEntity;

import java.io.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;
import org.json.JSONArray;
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
                @RequestParam(required = false, defaultValue = ",") String delimiter,
                @RequestParam(required = false) String formatJsonPath) {
            
            // 生成作业ID
            String jobId = "job-" + System.currentTimeMillis();
            
            // 记录作业状态
            JobStatus status = new JobStatus(jobId, "SUBMITTED", inputFile, outputFile);
            jobStatusMap.put(jobId, status);
            
            // 异步执行Spark作业
            CompletableFuture.runAsync(() -> {
                try {
                    logger.info("Starting CSV processing job: {}", jobId);
                    jobStatusMap.get(jobId).setStatus("RUNNING");
                    
                    // 解析formatJson获取表头信息
                    List<String> headers = new ArrayList<>();
                    String tempOutputFile = outputFile + ".tmp";
                    
                    if (formatJsonPath != null && !formatJsonPath.isEmpty()) {
                        try {
                            String jsonContent = new String(Files.readAllBytes(Paths.get(formatJsonPath)));
                            JSONObject formatJson = new JSONObject(jsonContent);
                            if (formatJson.has("headers")) {
                                JSONArray headersArray = formatJson.getJSONArray("headers");
                                for (int i = 0; i < headersArray.length(); i++) {
                                    headers.add(headersArray.getString(i));
                                }
                            }
                        } catch (Exception e) {
                            logger.error("Error parsing format.json: " + e.getMessage(), e);
                        }
                    }
                    
                    // 准备执行Spark提交命令
                    String sparkSubmitCmd = buildSparkSubmitCommand(
                            inputFile, 
                            tempOutputFile, 
                            fields, 
                            delimiter);
                    
                    logger.info("Executing command: {}", sparkSubmitCmd);
                    
                    // 执行命令
                    Process process = Runtime.getRuntime().exec(sparkSubmitCmd);
                    int exitCode = process.waitFor();
                    
                    if (exitCode != 0) {
                        // 读取错误输出
                        try (BufferedReader reader = new BufferedReader(
                                new InputStreamReader(process.getErrorStream()))) {
                            StringBuilder errorOutput = new StringBuilder();
                            String line;
                            while ((line = reader.readLine()) != null) {
                                errorOutput.append(line).append("\n");
                            }
                            throw new RuntimeException("Spark job failed with exit code " + 
                                exitCode + ": " + errorOutput.toString());
                        }
                    }
                    
                    // 如果有表头信息，添加到输出文件
                    if (!headers.isEmpty()) {
                        addHeaderToOutputFile(tempOutputFile, outputFile, headers, delimiter);
                    } else {
                        // 没有表头，直接重命名文件
                        Files.move(Paths.get(tempOutputFile), Paths.get(outputFile));
                    }
                    
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
        
        private String buildSparkSubmitCommand(String inputFile, String outputFile, 
                                             String fields, String delimiter) {
            StringBuilder cmd = new StringBuilder();
            
            // 构建spark-submit命令
            cmd.append("spark-submit ");
            cmd.append("--class com.example.beam.SparkBeamCsvProcessor ");
            cmd.append("--master local[*] ");  // 本地模式，可以根据需要修改
            
            // JAR包路径 - 需要配置为实际路径
            cmd.append("/path/to/spark-beam-csv-processor-1.0-SNAPSHOT.jar ");
            
            // 添加参数
            cmd.append("--inputFile=").append(inputFile).append(" ");
            cmd.append("--outputFile=").append(outputFile).append(" ");
            cmd.append("--fields=").append(fields).append(" ");
            cmd.append("--delimiter=").append(delimiter);
            
            return cmd.toString();
        }
        
        private void addHeaderToOutputFile(String inputFile, String outputFile, 
                                         List<String> headers, String delimiter) throws IOException {
            // 创建包含表头的新文件
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
                // 写入表头
                writer.write(String.join(delimiter, headers));
                writer.newLine();
                
                // 写入原始数据
                Files.lines(Paths.get(inputFile)).forEach(line -> {
                    try {
                        writer.write(line);
                        writer.newLine();
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
            }
            
            // 删除临时文件
            Files.deleteIfExists(Paths.get(inputFile));
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

