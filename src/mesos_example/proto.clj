(ns mesos-example.proto
  (:use [mesos-example.protobuf])
  (:import org.apache.mesos.Protos))


;;; See also: MESOS/include/mesos/mesos.proton

(defprotobox OfferId
  org.apache.mesos.Protos$OfferID
  [value])

(defprotobox FrameworkId
  org.apache.mesos.Protos$FrameworkID
  [value])

(defprotobox SlaveId
  org.apache.mesos.Protos$SlaveID
  [value])

(defprotobox TaskId
  org.apache.mesos.Protos$TaskID
  [value])

(defprotobox ExecutorId
  org.apache.mesos.Protos$ExecutorID
  [value])

(def SCALAR org.apache.mesos.Protos$Value$Type/SCALAR)
(def RANGES org.apache.mesos.Protos$Value$Type/RANGES)

(defprotobox Scalar
  org.apache.mesos.Protos$Value$Scalar
  [value])

(defproto Range
  org.apache.mesos.Protos$Value$Range
  [begin]
  [end])

(defprotobox Ranges
  org.apache.mesos.Protos$Value$Ranges
  [range [Range]])

(defproto FrameworkInfo
  org.apache.mesos.Protos$FrameworkInfo
  [user]
  [name]
  [id FrameworkId]
  [failover-timeout]
  [checkpoint]
  [role]
  [hostname]
  [principal])

(defproto Resource
  org.apache.mesos.Protos$Resource
  [name]
  [type]
  [scalar Scalar]
  [ranges Ranges])

(defproto Offer
  org.apache.mesos.Protos$Offer
  [id OfferId]
  [framework-id FrameworkId]
  [slave-id SlaveId]
  [hostname]
  [resources [Resource]])

(defproto Request
  org.apache.mesos.Protos$Request
  [slave-id SlaveId]
  [resources [Resource]])

(defproto URI
  org.apache.mesos.Protos$CommandInfo$URI
  [value]
  [executable]
  [extract])

(defproto CommandInfo
  org.apache.mesos.Protos$CommandInfo
  [uris [URI]]
  [shell]
  [value]
  [arguments []])

(defproto ExecutorInfo
  org.apache.mesos.Protos$ExecutorInfo
  [executor-id ExecutorId]
  [framework-id FrameworkId]
  [command CommandInfo]
  [resources [Resource]]
  [name]
  [source])

(defproto TaskInfo
  org.apache.mesos.Protos$TaskInfo
  [name]
  [task-id TaskId]
  [slave-id SlaveId]
  [resources [Resource]]
  [executor ExecutorInfo]
  [command CommandInfo])

(def RUNNING org.apache.mesos.Protos$TaskState/TASK_RUNNING)
(def FINISHED org.apache.mesos.Protos$TaskState/TASK_FINISHED)
(def FAILED org.apache.mesos.Protos$TaskState/TASK_FAILED)
(def KILLED org.apache.mesos.Protos$TaskState/TASK_KILLED)

(defproto TaskStatus
  org.apache.mesos.Protos$TaskStatus
  [task-id TaskId]
  [state]
  [message]
  [slave-id SlaveId]
  [executor-id ExecutorId]
  [timestamp]
  [healthy])



;;; Helpers

(defn offer-id-key
  [id]
  (symbol (.getValue id)))

(defn offer-key
  [offer]
  (offer-id-key (.getId offer)))

(defn resource-value
  [resource]
  (let [type (:type resource)]
    (cond
     (= type SCALAR) (:scalar resource)
     (= type RANGES) (:ranges resource))))

(defn offer-resources
  [offer]
  (into
   {}
   (map
    (fn [resource]
      [(keyword (:name resource)) (resource-value resource)])
    (:resources (parse offer)))))

(defn resource
  [key value]
  (Resource :name (name key) :type SCALAR :scalar value))

(defn task-info
  [task-id slave-id executor &{:keys [mem cpus] :as options}]
  (TaskInfo
   :name (str "task " (.getValue task-id))
   :task-id task-id
   :slave-id slave-id
   :executor executor
   :resources [(resource :cpus cpus) (resource :mem mem)]))
