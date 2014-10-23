(ns mesos-example.primes
  (:use
   [mesos-example.scheduler]
   [mesos-example.executor])
  (:require
   [clojure.pprint :as pp]
   [primitive-math :as p])
  (:gen-class))

(defn prime-seq
  "Create a sequence of prime numbers less than LIMIT. The GENERATE
  method starts at 3. This wrapper sanity-checks LIMIT and begins the
  sequence of primes at 2."
  [generate limit & more]
  (when (p/> limit 2)
    (cons 2 (apply generate limit more))))


;;; Numbers
;;;
;;; By default, the run time of the prime number generators below are
;;; dominated by overhead Clojure's numeric tower. In the interest of
;;; speed, use primitive Java math operations instead.
;;;
;;; Also, use type hints to reduce the number of runtime casts.
;;;
;;; This means overflow should be a consideration when choosing a
;;; LIMIT for the ADDITIVE? test.

(defn div-ceil
  "Perform integer division, but round up instead of truncating the
  remainder."
  [^long n1 ^long n2]
  (if (p/== 0 (p/rem n1 n2))
    (p/div n1 n2)
    (p/inc (p/div n1 n2))))

(defn prim-odd?
  "Return true when NUMBER is odd."
  [^long number]

  (p/== 1 (p/rem number 2)))

(defn odd
  "Find the odd value greater than or equal to NUMBER."
  [^long number]
  (if (prim-odd? number) number (p/inc number)))

(defn next-odd
  "Find the next odd value greater than NUMBER."
  [^long number]
  (if (prim-odd? number)
    (p/+ number 2)
    (p/+ number 1)))

(defn prim-range
  "Like RANGE, but uses primitive math and `long` typehints."
  ([^long start ^long end] (prim-range start end 1))
  ([^long start ^long end ^long step]
   (lazy-seq
    (let [buffer (chunk-buffer 32)]
      (loop [index start]
        (let [size (count buffer)]
          (if (and (p/< size 32) (p/< index end))
            (do
              (chunk-append buffer index)
              (recur (p/+ index step)))
            (chunk-cons (chunk buffer)
                        (when (p/< index end)
                          (prim-range index end step))))))))))

(defn irange
  "Like RANGE, but END is inclusive."
  ([^long start ^long end] (irange start end 1))
  ([^long start ^long end ^long step]
     (prim-range start (p/inc end) step)))

(defn prim-sqrt
  "Return the truncated square root of NUMBER."
  [^long number]
  (p/long (Math/sqrt (p/double number))))


;;; Sieve
;;;
;;; Generate a sequence of prime numbers using the Sieve of
;;; Eratosthenes. This approach has a much better time complexity than
;;; trial division.
;;;
;;; The strategy is to initially assume all numbers are
;;; prime. Starting with 3 up to the square root of the limit,
;;; "unflag" all multiples of the current prime in a BitSet. Skip to
;;; the next prime using the BitSet to identify composite
;;; numbers. Flag multiples of it, &c.
;;;
;;; Optimizations:
;;;
;;; + Only track odd numbers, reduces memory space and
;;;   time-complexity.
;;; + Emit primes as they are discovered in a lazy sequence rather
;;;   than in a final loop with the BitSet.

(definline number->index
  "Given a number, find its BitSet index."
  [number]
  ;; Only odd numbers are tracked, so to get the actual number of
  ;; bits, divide by two. Subtract one because the loop starts at
  ;; three instead of one.
  `(p/- (p/div ~number 2) 1))

(defn sieve-bitset
  "Create a BitSet for the given LIMIT. Alternative, specify a START
  and END. This is useful for the parallelized sieve."
  ([limit] (sieve-bitset 3 limit))
  ([^long start ^long end]
     (let [end-index (number->index end)
           start-index (number->index start)
           size (p/inc (p/- end-index start-index))]
       (doto (java.util.BitSet. size)
         (.set 0 (p/int size))))))

(defn sieve-prime?
  "Given a BitSet, FLAGS, determine if NUMBER is prime."
  [^java.util.BitSet flags ^long number]
  (.get flags (p/int (number->index number))))

(defn mark-composites
  "Unflag all multiples of PRIME up to LIMIT."
  [^java.util.BitSet flags ^long prime ^long limit]
  (doseq [^long composite (irange (p/* prime prime) limit (p/* 2 prime))]
    (.clear flags (p/int (number->index composite))))
  prime)

(defn first-sieve-range
  "Generate a sequence of numbers up to the square root of LIMIT. A
  sieve is created by marking multiples of prime numbers in this
  range."
  [limit]
  (irange 3 (prim-sqrt limit) 2))

(defn rest-sieve-range
  "Generate a sequence of numbers past the square root of LIMIT. Once
  the sieve is created, numbers in this range are known to be prime or
  composite."
  [limit]
  (prim-range (next-odd (prim-sqrt limit)) limit 2))

(defn lazy-filter
  "Like FILTER, but even lazier. The built-in FILTER uses chunking;
  since the sieve side-effects the predicate, chunking allows bad
  numbers through too early."
  [pred? [first & more :as seq]]
  (lazy-seq
   (cond
    (empty? seq) nil
    (pred? first) (cons first (lazy-filter pred? more))
    :default (lazy-filter pred? more))))

(defn lazy-map
  "Like MAP, but even lazier. See note in LAZY-FILTER."
  [fn seq]
  (lazy-seq
   (when-not (empty? seq)
     (cons (fn (first seq))
           (lazy-map fn (rest seq))))))

(defn- sieve-generator
  "Generate prime numbers starting from 3 up to LIMIT using a sieve."
  [limit]
  (let [flags (sieve-bitset limit)
        prime? #(sieve-prime? flags %)
        mark #(mark-composites flags % limit)]
    (lazy-cat
     (lazy-map mark (lazy-filter prime? (first-sieve-range limit)))
     (filter prime? (rest-sieve-range limit)))))

(defn sieve-primes
  "Produce a sequence of prime numbers from 2 to LIMIT using the Sieve
  of Eratosthenes."
  [limit]
  (prime-seq sieve-generator limit))


;;; Parallel Sieve
;;;
;;; The Sieve of Eratosthenes can be parallelized by creating the
;;; sieve in a "controller" thread. Worker threads are given a
;;; partition of the remaining number space past the square root of
;;; limit. They are responsible for receiving primes discovered by the
;;; controller and sieving their partition.
;;;
;;; Accomplish this by using a Clojure serial queue (seque) as the
;;; controller. It acts as a lazy sequence, but blocks workers until
;;; additional items are available. Workers are spun up by PMAP and
;;; their results are reduced together using flatten and concatenated
;;; onto the primes from the controller queue.

(defn rest-sieve-partition
  "Produce a sequence of [start, end] partitions over the space of
  numbers in the range 3..LIMIT. Each partition has the magnitude
  SIZE. This is an optimization to reduce overhead from PARTITION-ALL
  and RANGE."
  ([^long size ^long limit]
     (rest-sieve-partition size (next-odd (prim-sqrt limit)) limit))
  ([^long size ^long start ^long limit]
     (lazy-seq
      (when (p/< start limit)
        (let [end (p/min (p/+ start size) limit)]
          (cons [(odd start) end]
                (rest-sieve-partition size end limit)))))))

(defn sieve-partition-prime?
  "Given the FLAGS for a partition and BASE index, is NUMBER prime?"
  [^java.util.BitSet flags ^long number ^long base]
  (let [index (number->index number)]
    (.get flags (p/int (p/- index base)))))

(defn first-odd-multiple
  "Find the first odd multiple of PRIME greater than or equal to
  START. This is used to start flagging composites in a partition."
  [^long prime ^long start]
  (let [^long lower-bound (div-ceil start prime)
        probe (p/* prime lower-bound)]
    (if (prim-odd? probe)
      probe
      (p/+ probe prime))))

(defn mark-partition-composites!
  "Mark multiples of PRIME in the partition space given by START and
  END."
  [^java.util.BitSet flags ^long prime ^long start ^long end]
  (let [base (number->index start)
        ^long first-multiple (first-odd-multiple prime start)]
    (doseq [^long composite (prim-range first-multiple end (p/* 2 prime))]
      (let [index (number->index composite)]
        (.clear flags (p/int (p/- index base)))))))

(defn apply-sieve
  "Given CONTROL, a seque of sieve primes, apply the primes to the
  partition START..END. Once SEED is exhausted, produce a sequence of
  primes in the partition space."
  [control [^long start ^long end]]
  (let [flags (sieve-bitset start end)
        base (number->index start)
        prime? #(sieve-partition-prime? flags % base)]

    (doseq [^long prime control]
      (mark-partition-composites! flags prime start end))

    (filter prime? (prim-range start end 2))))

(defn- parallel-sieve-generator
  "Generate a sequence of primes from 3 up to LIMIT. The
  PARTITION-SIZE parameter can be used to tune parallelization."
  [limit partition-size]
  (let [control-end (prim-sqrt limit)
        flags (sieve-bitset control-end)
        prime? #(sieve-prime? flags %)
        mark #(mark-composites flags % control-end)
        control (seque (lazy-map mark (lazy-filter prime? (first-sieve-range limit))))]
    (lazy-cat
     control
     (flatten
      (pmap
       #(apply-sieve control %)
       (rest-sieve-partition partition-size limit))))))

(defn parallel-sieve-primes
  "Produce a sequence of primes from 2 up to LIMIT by applying the
  Sieve of Eratosthenes in parallel. Optionally provide a
  PARTITION-SIZE to tune parallelization."
  ([limit] (parallel-sieve-primes limit 32768))
  ([limit partition-size]
     (prime-seq parallel-sieve-generator limit partition-size)))


;;; Cluster

(defn distribute-seive
  [scheduler seive partition-size limit]
  (lazy-cat
   seive
   (flatten
    (pmap
     (fn [partition]
       @(schedule! scheduler [:apply-seive seive partition]))
     (rest-sieve-partition partition-size limit)))))

(defn cluster
  []
  (let [scheduler (start-scheduler)
        limit 2000
        partition-size 32768
        seive @(schedule! scheduler [:make-seive limit])]
    (doseq [primes (partition 20 (distribute-seive scheduler seive partition-size limit))]
      (println :some-primes primes))))


;;; Fork

(defn make-seive
  [limit]
  (let [control-end (prim-sqrt limit)
        flags (sieve-bitset control-end)
        prime? #(sieve-prime? flags %)
        mark #(mark-composites flags % control-end)]
    (map mark (filter prime? (first-sieve-range limit)))))

(defn dispatch-work
  [method & args]
  (case method
    :make-seive (apply make-seive args)
    :apply-seive (apply apply-sieve args)))

(defn fork
  []
  (let [executor (start-executor dispatch-work)]
    (.join executor)))

(defn -main
  [mode & args]
  (case mode
    "cluster" (cluster)
    "fork!"   (fork)))
