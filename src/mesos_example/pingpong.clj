(ns mesos-example.pingpong
  (:use
   [mesos-example.scheduler]
   [mesos-example.executor])
  (:require
   [clojure.pprint :as pp])
  (:gen-class))


;;; Cluster

(defn cluster
  []
  (let [scheduler (start-scheduler)]
    (loop []
      (println ::received @(schedule! scheduler [:ping]))
      (Thread/sleep 1000)
      (recur))))


;;; Fork

(defn dispatch-work
  [method & args]
  (case method
    :ping :pong))

(defn fork
  []
  (let [executor (start-executor dispatch-work)]
    (.join executor)))

(defn -main
  [mode & args]
  (case mode
    "cluster" (cluster)
    "fork!"   (fork)))
