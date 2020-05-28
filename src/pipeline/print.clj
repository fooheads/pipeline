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

(defn print-result
  "Prints the result of the last or given in a human friendly way."
  ([] (print-result (pipeline/last-result)))
  ([result]
   (if (pipeline/success? result)
     (println (format "Success!\n\n%s" (pr-str (pipeline/get-output result))))
     (print-error (pipeline/get-error result)))))

(defn print-failed-call
  "Prints the call that failed as close as possible to what a conctete call
  would look like. Normally, this output can be pasted into the REPL and
  executed for further debugging"
  ([] (print-failed-call (pipeline/last-result)))
  ([result]
   (when (pipeline/failure? result)
     (let [step (-> result :pipeline/error)
           f (:pipeline.step/function step)
           input-paths (:pipeline.step/input-paths step)
           args (map #(get-in result %) input-paths)]
       (prn "args: " args)
       (if-not (empty? args)
         (println (str "(" f " " (str/join " " (map pr-str args)) ")"))
         (println (str "(" f ")")))))))


