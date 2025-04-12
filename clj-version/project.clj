(defproject csv-beam-processor "0.1.0-SNAPSHOT"
  :description "CSV处理器 - 使用Apache Beam筛选CSV字段"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [org.clojure/data.csv "1.0.1"]
                 [org.apache.beam/beam-sdks-java-core "2.46.0"]
                 [org.apache.beam/beam-runners-direct-java "2.46.0"]
                 [org.slf4j/slf4j-api "2.0.7"]
                 [org.slf4j/slf4j-simple "2.0.7"]]
  :main ^:skip-aot csv-processor.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all
                       :jvm-opts ["-Dclojure.compiler.direct-linking=true"]}})

