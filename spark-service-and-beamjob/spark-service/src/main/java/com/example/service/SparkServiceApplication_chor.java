package com.example.service;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.web.bind.annotation.*;
import org.springframework.http.ResponseEntity;
import org.springframework.jms.annotation.EnableJms;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;
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
import com.fasterxml.jackson.databind.ObjectMapper;

@SpringBootApplication
@EnableJms
public class SparkServiceApplication {

    private static final Logger logger = LoggerFactory.getLogger(SparkServiceApplication.class);
    
    public static void main(String[] args) {
        SpringApplication.run(SparkServiceApplication.class, args);
    }
    
    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }

    @RestController
    @RequestMapping("/api/spark")
    public static class SparkController {
        
        private final ExecutorService executorService = Executors.newFixedThreadPool(5);
        private final Map<String, JobStatus> jobStatusMap = new HashMap<>();
        
        @Autowired
        private JmsTemplate jmsTemplate;
        
        @Autowired
        private ObjectMapper objectMapper;
        
        @Value("${sparkapp.jar.path:/path/to/spark-beam-csv-processor-1.0-SNAPSHOT.jar}")
        private String sparkAppJarPath;
        
        @Value("${chor.job.status.queue:job-status-queue}")
        private String jobStatusQueue;
        
        @Value("${chor.job.result.queue:job-result-queue}")
        private String jobResultQueue;
        
        /**
         * 接收CHOR消息来触发作业
         */
        @JmsListener(destination = "${chor.job.request.queue:job-request-queue}")
        public void receiveJobRequest(Message message) throws JMSException {
            if (message instanceof TextMessage) {
                String payload = ((TextMessage) message).getText();
                logger.info("Received job request: {}", payload);
                
                try {
                    // 解析CHOR消息
                    JSONObject request = new JSONObject(payload);
                    String requestType = request.getString("type");
                    
                    if ("START_JOB".equals(requestType)) {
                        // 处理启动作业请求
                        String inputFile = request.getString("inputFile");
                        String outputFile = request.getString("outputFile");
                        String fields = request.optString("fields", "0,1,2");
                        String delimiter = request.optString("delimiter", ",");
                        String formatJsonPath = request.optString("formatJsonPath", "");
                        
                        // 启动作业
                        Map<String, Object> response = startJob(inputFile, outputFile, fields, delimiter, formatJsonPath);
                        
                        // 发送确认消息
                        sendJobStatusMessage((String)response.get("jobId"), "SUBMITTED", "Job submitted successfully");
                    } else if ("GET_JOB_STATUS".equals(requestType)) {
                        // 处理查询作业状态请求
                        String jobId = request.getString("jobId");
                        JobStatus status = jobStatusMap.get(jobId);
                        
                        if (status != null) {
                            // 发送作业状态
                            sendJobStatusMessage(jobId, status.getStatus(), status.getErrorMessage());
                        } else {
                            // 作业不存在
                            sendJobStatusMessage(jobId, "NOT_FOUND", "Job not found");
                        }
                    } else if ("GET_ALL_JOBS".equals(requestType)) {
                        // 处理查询所有作业请求
                        sendAllJobsStatusMessage();
                    }
                } catch (Exception e) {
                    logger.error("Error processing job request", e);
                    // 发送错误消息
                    JSONObject errorMsg = new JSONObject();
                    errorMsg.put("type", "ERROR");
                    errorMsg.put("message", e.getMessage());
                    jmsTemplate.convertAndSend(jobStatusQueue, errorMsg.toString());
                }
            }
        }
        
        /**
         * 发送作业状态消息
         */
        private void sendJobStatusMessage(String jobId, String status, String message) {
            try {
                JSONObject statusMsg = new JSONObject();
                statusMsg.put("type", "JOB_STATUS");
                statusMsg.put("jobId", jobId);
                statusMsg.put("status", status);
                if (message != null) {
                    statusMsg.put("message", message);
                }
                
                // 添加时间戳
                statusMsg.put("timestamp", System.currentTimeMillis());
                
                // 发送到CHOR队列
                jmsTemplate.convertAndSend(jobStatusQueue, statusMsg.toString());
                logger.info("Sent job status message for job: {}", jobId);
            } catch (Exception e) {
                logger.error("Error sending job status message", e);
            }
        }
        
        /**
         * 发送所有作业状态消息
         */
        private void sendAllJobsStatusMessage() {
            try {
                JSONObject statusMsg = new JSONObject();
                statusMsg.put("type", "ALL_JOBS_STATUS");
                
                JSONArray jobsArray = new JSONArray();
                for (Map.Entry<String, JobStatus> entry : jobStatusMap.entrySet()) {
                    JobStatus status = entry.getValue();
                    JSONObject jobObj = new JSONObject();
                    jobObj.put("jobId", status.getJobId());
                    jobObj.put("status", status.getStatus());
                    jobObj.put("inputFile", status.getInputFile());
                    jobObj.put("outputFile", status.getOutputFile());
                    jobObj.put("startTime", status.getStartTime());
                    jobObj.put("duration", status.getDuration());
                    if (status.getErrorMessage() != null) {
                        jobObj.put("errorMessage", status.getErrorMessage());
                    }
                    jobsArray.put(jobObj);
                }
                
                statusMsg.put("jobs", jobsArray);
                statusMsg.put("timestamp", System.currentTimeMillis());
                
                // 发送到CHOR队列
                jmsTemplate.convertAndSend(jobStatusQueue, statusMsg.toString());
                logger.info("Sent all jobs status message");
            } catch (Exception e) {
                logger.error("Error sending all jobs status message", e);
            }
        }
        
        /**
         * 发送作业完成结果消息
         */
        private void sendJobResultMessage(String jobId, String outputFile, boolean success, String errorMessage) {
            try {
                JSONObject resultMsg = new JSONObject();
                resultMsg.put("type", "JOB_RESULT");
                resultMsg.put("jobId", jobId);
                resultMsg.put("success", success);
                resultMsg.put("outputFile", outputFile);
                
                if (errorMessage != null) {
                    resultMsg.put("errorMessage", errorMessage);
                }
                
                // 添加时间戳
                resultMsg.put("timestamp", System.currentTimeMillis());
                
                // 发送到CHOR队列
                jmsTemplate.convertAndSend(jobResultQueue, resultMsg.toString());
                logger.info("Sent job result message for job: {}", jobId);
            } catch (Exception e) {
                logger.error("Error sending job result message", e);
            }
        }
        
        /**
         * REST API入口点
         */
        @PostMapping("/process-csv")
        public ResponseEntity<Map<String, Object>> processCsv(
                @RequestParam String inputFile,
                @RequestParam String outputFile,
                @RequestParam(required = false, defaultValue = "0,1,2") String fields,
                @RequestParam(required = false, defaultValue = ",") String delimiter,
                @RequestParam(required = false) String formatJsonPath) {
            
            Map<String, Object> response = startJob(inputFile, outputFile, fields, delimiter, formatJsonPath);
            return ResponseEntity.ok(response);
        }
        
        /**
         * 启动作业的核心逻辑
         */
        private Map<String, Object> startJob(String inputFile, String outputFile, String fields, 
                                         String delimiter, String formatJsonPath) {
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
                    sendJobStatusMessage(jobId, "RUNNING", null);
                    
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
                    
                    // 发送作业完成消息
                    sendJobStatusMessage(jobId, "COMPLETED", null);
                    sendJobResultMessage(jobId, outputFile, true, null);
                    
                } catch (Exception e) {
                    jobStatusMap.get(jobId).setStatus("FAILED");
                    jobStatusMap.get(jobId).setErrorMessage(e.getMessage());
                    logger.error("Job failed: " + jobId, e);
                    
                    // 发送作业失败消息
                    sendJobStatusMessage(jobId, "FAILED", e.getMessage());
                    sendJobResultMessage(jobId, outputFile, false, e.getMessage());
                }
            }, executorService);
            
            // 返回作业ID和状态
            Map<String, Object> response = new HashMap<>();
            response.put("jobId", jobId);
            response.put("status", "SUBMITTED");
            response.put("message", "CSV processing job submitted successfully");
            
            return response;
        }
        
        private String buildSparkSubmitCommand(String inputFile, String outputFile, 
                                             String fields, String delimiter) {
            StringBuilder cmd = new StringBuilder();
            
            // 构建spark-submit命令
            cmd.append("spark-submit ");
            cmd.append("--class com.example.beam.SparkBeamCsvProcessor ");
            cmd.append("--master local[*] ");  // 本地模式，可以根据需要修改
            
            // JAR包路径 - 从配置中获取
            cmd.append(sparkAppJarPath).append(" ");
            
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
