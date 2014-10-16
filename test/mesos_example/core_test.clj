(ns mesos-example.core-test
  (:require
   [clojure.test :refer :all]
   [mesos-example.proto :as proto]
   [mesos-example.core :refer :all]))

(defn default-scheduler-state
  []
  (-> (scheduler-state)
      (add-offers [(proto/make-offer "offer-1" "framework-20" "slave-3" "some.host.name")])))

(deftest test-add-offers
  (let [state (default-scheduler-state)]
    (assert (= (keys (:offers state))
               ['offer-1]))))

(deftest test-remove-offer
  (let [state (remove-offer (default-scheduler-state) (proto/make-offer-id "offer-1"))]
    (assert (= (keys (:offers state))
               nil))))
