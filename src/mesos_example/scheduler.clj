(ns mesos-example.scheduler
  (:use
   [mesos-example.proto]
   [mesos-example.protobuf])
  (:require
   [clojure.pprint :as pp]
   [taoensso.nippy :as nippy])
  (:import
   [org.apache.mesos Scheduler MesosSchedulerDriver]))


;;; Scheduler State

(defrecord SchedulerState [driver pump! framework-id master-info work-queue pool idle busy])

(defn scheduler-state
  []
  (SchedulerState. nil nil nil nil (clojure.lang.PersistentQueue/EMPTY) {} #{} #{}))

(defn- add-task
  [state task-status]
  (let [task (parse task-status)
        id (:task-id task)
        pool (:pool state)
        idle (:idle state)]
    (-> state
        (assoc-in [:pool id] task)
        (assoc :idle (conj idle id)))))

(defn- remove-task
  [state task-status]
  (let [id (:task-id (parse task-status))
        pool (:pool state)
        idle (:idle state)
        busy (:busy state)]
    (-> state
        (assoc :pool (dissoc pool id))
        (assoc :idle (disj idle id))
        (assoc :busy (disj busy id)))))

(defn- find-idle-task
  [state]
  (if-let [id (first (:idle state))]
    (get (:pool state) id)))

(defn- take-task
  [state id message-handler]
  (let [idle (:idle state)
        busy (:busy state)]
    (-> state
        (assoc :idle (disj idle id))
        (assoc :busy (conj busy id))
        (assoc-in [:pool id :message-handler] message-handler))))

(defn- task-message-handler
  [state id]
  (get-in state [:pool id :message-handler]))

(defn- free-task
  [state id]
  (let [idle (:idle state)
        busy (:busy state)]
    (-> state
        (assoc :idle (conj idle id))
        (assoc :busy (disj busy id))
        (assoc-in [:pool id :message-handler] nil))))


;;; Work

(defrecord UnitOfWork [op promise])

(defn unit-of-work
  [op]
  (UnitOfWork. op (promise)))

(defn fulfill-work!
  [unit result]
  ((:promise unit) result)
  unit)

(defn schedule-work
  [scheduler-state unit]
  (let [queue (:work-queue scheduler-state)]
    (assoc scheduler-state :work-queue (conj queue unit))))

(defn schedule!
  [scheduler op]
  (let [unit (unit-of-work op)]
    (swap! scheduler schedule-work unit)
    ((:pump! @scheduler) (:driver @scheduler))
    (:promise unit)))


;;; Scheduler

(defn reify-scheduler
  "http://mesos.apache.org/api/latest/java/org/apache/mesos/Scheduler.html"
  [state-atom executor]
  (let [launched (atom 0)

        perform (fn [driver task work]
                  (.sendFrameworkMessage
                   driver
                   (.getExecutorId executor)
                   (SlaveId (:slave-id task))
                   (nippy/freeze {:perform (:op work) :task-id (:task-id task)})))

        pump (fn [driver]
               (swap! state-atom
                      (fn [state]
                        (let [queue (:work-queue state)
                              idle (find-idle-task state)
                              unit (first queue)]
                          (if (and idle unit)
                            (do
                              (perform driver idle unit)
                              (assoc
                               (take-task state (:task-id idle) unit)
                               :work-queue (pop queue)))
                            state)))))]

    (swap! state-atom assoc :pump! pump)

    (reify
      Scheduler

      ;; Invoked when the scheduler becomes "disconnected" from the
      ;; master (e.g., the master fails and another is taking over).
      (disconnected
        [this driver]
        ;; FIXME: clear all tasks, re-queue all the work.
        (println ::disconnected driver))

      ;; Invoked when there is an unrecoverable error in the scheduler
      ;; or driver.
      (error
        [this driver message]
        ;; FIXME: shut down, exit.
        (println ::error driver message))

      ;; Invoked when an executor has exited/terminated.
      (executorLost
        [this driver executor-id slave-id status]
        ;; FIXME: dunno...?
        (println ::executor-lost driver executor-id slave-id slave-id))

      ;; Invoked when an executor sends a message.
      (frameworkMessage
        [this driver executor-id slave-id data]
        (let [message (nippy/thaw data)
              task-id (:task-id message)
              unit (task-message-handler @state-atom task-id)]
          ;; FIXME: handle errors
          (swap! state-atom free-task task-id)
          (fulfill-work! unit (:result message))
          (pump driver)))

      ;; Invoked when an offer is no longer valid (e.g., the slave was
      ;; lost or another framework used resources in the offer).
      (offerRescinded
        [this driver offer-id]
        ;; FIXME: find out if this can happen for an offer used by an
        ;; existing task. If so, clear the task and re-queue the work.
        (println ::rescinded driver offer-id))

      ;; Invoked when the scheduler successfully registers with a Mesos
      ;; master.
      (registered
        [this driver framework-id master-info]
        ;; FIXME: ask for resource offers?
        (println ::registered driver framework-id master-info))

      ;; Invoked when the scheduler re-registers with a newly elected
      ;; Mesos master.
      (reregistered
        [this driver master-info]
        ;; FIXME: ask for resource offers?
        (println ::re-registered driver master-info))

      ;; Invoked when resources have been offered to this framework.
      (resourceOffers
        [this driver offers]
        (println ::resourceOffers (count offers))
        (doseq [offer offers]
          (let [{cpus :cpus mem :mem} (offer-resources offer)]
            (if (or (< cpus 1) (< mem 256))
              (.declineOffer driver (.getId offer))
              (let [id (TaskId (str (swap! launched inc)))]
                (println ::launching :offer-id (parse (.getId offer)) :task-id (parse id))
                (.launchTasks
                 driver
                 (.getId offer)
                 [(task-info id (.getSlaveId offer) executor :cpus 1 :mem 256)]
                 ))))))

      ;; Invoked when a slave has been determined unreachable (e.g.,
      ;; machine failure, network partition).
      (slaveLost
        [this driver slave-id]
        ;; FIXME: remove all tasks for this slave and re-queue the work
        (println ::slave-lost driver slave-id))

      ;; Invoked when the status of a task has changed (e.g., a slave is
      ;; lost and so the task is lost, a task finishes and an executor
      ;; sends a status update saying so, etc).
      (statusUpdate
        [this driver status]
        (let [state (.getState status)]
          (cond
           (= state RUNNING)
           (do
             (swap! state-atom add-task status)
             (pump driver))

           :else
           (swap! state-atom remove-task status)
           ))))))

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

    (let [driver
          (MesosSchedulerDriver.
           (reify-scheduler state-atom executor)
           (describe-framework principal (or name principal))
           master)]
      (swap! state-atom assoc :driver driver)
      state-atom)))

(defn start-scheduler
  []
  (let [driver
        (scheduler
         :master "127.0.1.1:5050"
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

(defn join-scheduler
  [scheduler]
  (.join (:driver @scheduler)))
