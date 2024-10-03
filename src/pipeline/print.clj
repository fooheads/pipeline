(ns pipeline.print
  "A namespace for printing pipelines, pipeline results
  and other related things in different ways."
  (:require
    [clojure.pprint :refer [print-table]]
    [clojure.string :as str]
    [pipeline.core :as pipeline]))


(defn print-call
  "Prints the call as close as possible to what a conctete call
  would look like. Normally, this output can be pasted into the REPL and
  executed for further debugging"
  [step]
  (let [f (:pipeline.step/function step)
        _input-paths (:pipeline.step/input-paths step)
        args (:pipeline.step/args step)]
    (if-not (empty? args)
      (println (str "(" f " " (str/join " " (map pr-str args)) ")"))
      (println (str "(" f ")")))))


(defn print-failed-call
  "Prints the call that failed as close as possible to what a conctete call
  would look like. Normally, this output can be pasted into the REPL and
  executed for further debugging"
  ([] (print-failed-call (pipeline/last-run)))
  ([result]
   (when (pipeline/failed? result)
     (let [step (pipeline/failed-step result)]
       (print-call step)))))


(defn ->short-str
  [len v]
  (let [s (str v)]
    (if (< (count s) len)
      s
      (format "%s..." (subs s 0 len)))))


(defn map-vals
  "Apply f to all values in m"
  [f m]
  (reduce-kv
    (fn [m k v]
      (assoc m k (f v))) {} m))


(defn map-keys
  "Apply f to all keys in m"
  [f m]
  (reduce-kv
    (fn [m k v]
      (assoc m (f k) v)) {} m))


(defn print-run
  ([] (print-run (pipeline/last-run)))
  ([run] (print-run run [:seq-id :state :name :function :args :result]))
  ([run ks]
   (->>
     run
     :pipeline/steps
     (map #(map-vals (partial ->short-str 35) %))
     (map #(map-keys (comp keyword name) %))
     (print-table ks))))

