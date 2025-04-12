(ns csv-processor.core
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.string :as str])
  (:import [org.apache.beam.sdk Pipeline]
           [org.apache.beam.sdk.io TextIO]
           [org.apache.beam.sdk.options PipelineOptions PipelineOptionsFactory]
           [org.apache.beam.sdk.transforms DoFn DoFn$ProcessContext ParDo Create]
           [org.apache.beam.sdk.values PCollection]
           [org.apache.beam.sdk.coders StringUtf8Coder])
  (:gen-class))

;; 定义 CSV 处理选项接口
(gen-interface
  :name "csv_processor.CSVProcessorOptions"
  :extends [org.apache.beam.sdk.options.PipelineOptions]
  :methods [[getInputFile [] String]
            [setInputFile [String] void]
            [getOutputFile [] String]
            [setOutputFile [String] void]
            [getFieldIndices [] String]
            [setFieldIndices [String] void]
            [getHasHeader [] boolean]
            [setHasHeader [boolean] void]])

;; CSV 解析器函数
(defn create-csv-parser-fn [field-indices header?]
  (proxy [DoFn] []
    (processElement [^DoFn$ProcessContext c]
      (let [line (.element c)
            parsed (try
                     (first (csv/read-csv line))
                     (catch Exception e
                       (println (str "警告: 无法解析行: " line))
                       []))]
        (when-not (empty? parsed)
          (try
            (let [selected (mapv #(get parsed % "") field-indices)
                  output-line (csv/write-csv [selected] :escape-strings? true)]
              (.output c (str/trim (first output-line))))
            (catch Exception e
              (println (str "处理字段时出错: " e)))))))))

;; 解析字段索引字符串，例如 "0,2,5" → [0 2 5]
(defn parse-field-indices [indices-str]
  (mapv #(Integer/parseInt %) (str/split (or indices-str "0") #",")))

(defn run-pipeline [options]
  (let [pipeline-options (doto (PipelineOptionsFactory/fromArgs (into-array String options))
                           (.as csv_processor.CSVProcessorOptions))
        input-file (.getInputFile pipeline-options)
        output-file (.getOutputFile pipeline-options)
        field-indices-str (.getFieldIndices pipeline-options)
        field-indices (parse-field-indices field-indices-str)
        has-header (.getHasHeader pipeline-options)
        pipeline (Pipeline/create pipeline-options)]
    
    (println (str "开始处理 CSV 文件: " input-file))
    (println (str "筛选字段索引: " field-indices))
    (println (str "输出文件: " output-file))
    (println (str "CSV 有头行: " has-header))
    
    (-> pipeline
        (.apply "ReadLines" (TextIO/read (io/file input-file)))
        (.apply "ParseCSV" (ParDo/of (create-csv-parser-fn field-indices has-header)))
        (.setCoder (StringUtf8Coder/of))
        (.apply "WriteCSV" (TextIO/write (io/file output-file))))
    
    (.run pipeline)
    (println "处理完成!")))

(defn -main [& args]
  (let [default-options ["--inputFile=input.csv"
                         "--outputFile=output.csv"
                         "--fieldIndices=0,2,5"
                         "--hasHeader=true"]
        all-options (if (empty? args) default-options args)]
    (run-pipeline all-options)))

