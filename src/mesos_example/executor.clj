(ns mesos-example.executor
  (:use
   [mesos-example.proto]
   [mesos-example.protobuf])
  (:require
   [clojure.pprint :as pp]
   [taoensso.nippy :as nippy])
  (:import
   [org.apache.mesos Executor MesosExecutorDriver]))

(defn reify-executor
  "http://mesos.apache.org/api/latest/java/org/apache/mesos/Executor.html"
  [dispatch]
  (reify
    Executor

    ;; Invoked when the executor becomes "disconnected" from the slave
    ;; (e.g., the slave is being restarted due to an upgrade)
    (disconnected
      [this driver]
      (println ::disconnected driver))

    ;; Invoked when a fatal error has occurred with the executor
    ;; and/or executor driver.
    (error
      [this driver message]
      (println ::error driver message))

    ;; Invoked when a framework message has arrived for this executor.
    (frameworkMessage
      [this driver data]
      (let [{perform :perform task-id :task-id} (nippy/thaw data)]
        (future
          (println ::executor-received-message driver perform task-id)
          (let [result (apply dispatch perform)]
            (.sendFrameworkMessage
             driver
             (nippy/freeze {:result result :task-id task-id}))))))

    ;; Invoked when a task running within this executor has been
    ;; killed (via SchedulerDriver.killTask(TaskID)).
    (killTask
      [this driver task-id]
      (println ::kill-task driver task-id))

    ;; Invoked when a task has been launched on this executor
    ;; (initiated via SchedulerDriver.launchTasks)
    (launchTask
      [this driver task-info]
      (future
        (println ::launch-task (.getTaskId task-info))
        (.sendStatusUpdate
         driver
         (TaskStatus :task-id (.getTaskId task-info) :state RUNNING))))

    ;; Invoked once the executor driver has been able to successfully
    ;; connect with Mesos.
    (registered
      [this driver executor-info framework-info slave-info]
      (println ::registered driver executor-info framework-info slave-info))

    ;; Invoked when the executor re-registers with a restarted slave.
    (reregistered
      [this driver slave-info]
      (println ::re-registered driver slave-info))

    ;; Invoked when the executor should terminate all of it's currently running tasks.
    (shutdown
      [this driver]
      (println ::shutdown driver))

    ))

(defn start-executor
  [dispatch]
  (let [driver (MesosExecutorDriver. (reify-executor dispatch))]
    (.start driver)
    driver))
