(ns pipeline.print
  "A namespace for printing pipelines, pipeline results
  and other related things in different ways."
  (:require
    [clojure.string :as str]
    [clojure.pprint :refer [pprint print-table]]
    [pipeline.core :as pipeline]))

(defn- print-exception [error]
  (println
    (format (str/join
              "\n"
              ["Error: Pipeline exited on step %s due to exception."
               "Reason: %s"
               "Message: %s"
               "Hint: Exception object can be found with (pipeline/get-exception)"])
            (:pipeline.step/name error)
            (:pipeline.error/reason error)
            (:pipeline.error/message error))))

(defn- print-validation-error [error]
  (println
    (format "Error: Pipeline exited on step %s due to %s.\nMessage: %s\nFailing value %s."
            (:pipeline.step/name error)
            (:pipeline.error/reason error)
            (:pipeline.error/message error)
            (pr-str (:pipeline.error/value error)))))

(defn- print-error [error]
  (if (= (:pipeline.error/reason error) :exception)
    (print-exception error)
    (print-validation-error error)))

#_(defn print-result
    "Prints the result of the last or given in a human friendly way."
    ([] (print-result (pipeline/last-run)))
    ([run]
     (if (pipeline/successful? run)
       (println (format "Success!\n\n%s" (pr-str (pipeline/result run))))
       (print-error (pipeline/error run)))))

(defn print-call
  "Prints the call as close as possible to what a conctete call
  would look like. Normally, this output can be pasted into the REPL and
  executed for further debugging"
  [step]
  (let [f (:pipeline.step/function step)
        input-paths (:pipeline.step/input-paths step)
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


(defn ->short-str [len v]
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

