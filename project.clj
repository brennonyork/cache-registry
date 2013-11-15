(defproject cache-registry "1.3.4"
  :description "Cache once, fail on filesystem registry"
  :url "https://github.com/brennonyork/cache-registry"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :plugins [[lein-javadoc "0.1.1"]]
  :javadoc-opts {:package-names ["org.cache.fs"]
                 :output-dir "docs"}
  :source-paths ["src/clj"]
  :java-source-paths ["src/java"]
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.apache.commons/commons-lang3 "3.1"]
                 [org.apache.hadoop/hadoop-core "1.0.4"]
                 [log4j "1.2.16"]]
  :aliases {"docs" "javadoc"
            "fresh" ["do" "clean," "uberjar"]})
