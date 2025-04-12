(ns csv-processor.core-test
  (:require [clojure.test :refer :all]
            [csv-processor.core :refer :all]))

(deftest parse-field-indices-test
  (testing "字段索引解析"
    (is (= [0 2 5] (parse-field-indices "0,2,5")))
    (is (= [0] (parse-field-indices "0")))
    (is (= [0] (parse-field-indices nil)))))

