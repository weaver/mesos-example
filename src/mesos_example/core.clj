(ns mesos-example.core
  (:use [mesos-example.proto])
  (:require [clojure.pprint :as pp])
  (:import
   [org.apache.mesos Scheduler MesosSchedulerDriver Executor MesosExecutorDriver]
   org.apache.mesos.Protos)
  (:gen-class))


;;; Scheduler State

(defrecord SchedulerState [driver framework-id master-info])

(defn scheduler-state
  []
  (SchedulerState. nil nil nil))

(defn decline-all-offers
  [state]
  (let [driver (:driver state)]
    (doseq [[key offer] (:offers state)]
      (.declineOffer driver (.getId offer)))))


;;; Scheduler

(defn reify-scheduler
  "http://mesos.apache.org/api/latest/java/org/apache/mesos/Scheduler.html"
  [state-atom executor]
  (let [launched (atom 0)]
    (reify
      Scheduler

      ;; Invoked when the scheduler becomes "disconnected" from the
      ;; master (e.g., the master fails and another is taking over).
      (disconnected
        [this driver]
        (println ::disconnected driver))

      ;; Invoked when there is an unrecoverable error in the scheduler
      ;; or driver.
      (error
        [this driver message]
        (println ::error driver message))

      ;; Invoked when an executor has exited/terminated.
      (executorLost
        [this driver executor-id slave-id status]
        (println ::executor-lost driver executor-id slave-id slave-id))

      ;; Invoked when an executor sends a message.
      (frameworkMessage
        [this driver executor-id slave-id data]
        (println ::framework-message driver executor-id slave-id data))

      ;; Invoked when an offer is no longer valid (e.g., the slave was
      ;; lost or another framework used resources in the offer).
      (offerRescinded
        [this driver offer-id]
        (println ::rescinded driver offer-id))

      ;; Invoked when the scheduler successfully registers with a Mesos
      ;; master.
      (registered
        [this driver framework-id master-info]
        (println ::registered driver framework-id master-info))

      ;; Invoked when the scheduler re-registers with a newly elected
      ;; Mesos master.
      (reregistered
        [this driver master-info]
        (println ::re-registered driver master-info))

      ;; Invoked when resources have been offered to this framework.
      (resourceOffers
        [this driver offers]
        (println ::offers driver offers)

        (doseq [offer offers]
          (let [{cpus :cpus mem :mem} (offer-resources offer)]
            (if (or (> @launched 0) (< cpus 1) (< mem 128))
              (.declineOffer driver (.getId offer))
              (let [id (TaskId (str (swap! launched inc)))]
                (println ::launching offer executor)
                (.launchTasks
                 driver
                 (.getId offer)
                 [(task-info id (.getSlaveId offer) executor :cpus 1 :mem 128)]
                 ))))))

      ;; Invoked when a slave has been determined unreachable (e.g.,
      ;; machine failure, network partition).
      (slaveLost
        [this driver slave-id]
        (println ::slave-lost driver slave-id))

      ;; Invoked when the status of a task has changed (e.g., a slave is
      ;; lost and so the task is lost, a task finishes and an executor
      ;; sends a status update saying so, etc).
      (statusUpdate
        [this driver status]
        (println ::status-update driver status)
        (if (= (.getState status) FINISHED)
          (.stop driver))))))

(defn describe-framework
  ([principal]
     (describe-framework principal principal))
  ([principal name]
     (FrameworkInfo
      :user ""
      :name name
      :principal principal
      :hostname "")))

(defn scheduler
  [&{ :keys [master principal name executor]}]
  (let [name (or name principal)
        state-atom (atom (scheduler-state))]

    (assert (not-empty master))
    (assert (not-empty principal))

    (swap! state-atom
           assoc :driver
           (MesosSchedulerDriver.
            (reify-scheduler state-atom executor)
            (describe-framework principal (or name principal))
            master))

    state-atom))

(defn- start-scheduler
  []
  (let [driver
        (scheduler
         :master "127.0.0.1:5050"
         :principal "example-clojure-framework"
         :executor
         (ExecutorInfo
          :executor-id "some-id"

          :command
          (CommandInfo
           :uris [(URI :value "/vagrant/mesos-example-0.1.0-SNAPSHOT-standalone.jar" :executable false :extract false)]
           :shell true
           :value "java -jar -Djava.library.path=/usr/local/lib mesos-example-0.1.0-SNAPSHOT-standalone.jar fork!")

          :name "Mesos Shell Executor"
          :source "mesos-example"))]

    (.start (:driver @driver))
    driver))


;;; Executor

(defn reify-executor
  "http://mesos.apache.org/api/latest/java/org/apache/mesos/Executor.html"
  []
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
      (println ::framework-message driver data))

    ;; Invoked when a task running within this executor has been
    ;; killed (via SchedulerDriver.killTask(TaskID)).
    (killTask
      [this driver task-id]
      (println ::kill-task driver task-id))

    ;; Invoked when a task has been launched on this executor
    ;; (initiated via SchedulerDriver.launchTasks)
    (launchTask
      [this driver task-info]
      (println ::launch-task)
      (future
        (try
          (.sendStatusUpdate
           driver
           (TaskStatus :task-id (.getTaskId task-info) :state RUNNING))

          (println ::task-running driver task-info)
          (Thread/sleep 3000)
          (println ::task-wrapping-up)

          (.sendStatusUpdate
           driver
           (TaskStatus :task-id (.getTaskId task-info) :state FINISHED))

          (catch Exception exc
            (println ::caught-exception exc)
            ))
        ))

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

(defn- start-executor
  []
  (let [driver (MesosExecutorDriver. (reify-executor))]
    (.run driver)))

(defn -main
  [mode & args]
  (case mode
    "cluster" (apply start-scheduler args)
    "fork!" (apply start-executor args)))
