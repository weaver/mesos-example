(defproject mesos-example "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.apache.mesos/mesos "0.20.0"]
                 [compojure "1.1.9"]]
  :jvm-opts ["-Xmx1g" "-Djava.library.path=/usr/local/lib"]
  :main mesos-example.core)