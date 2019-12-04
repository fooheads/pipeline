(ns pipeline.print
  "A namespace for printing pipelines, pipeline results
  and other related things in different ways."
  (:require
    [clojure.string :as str]
    [clojure.pprint :refer [pprint print-table]]
    [malli.core :as m]
    [pipeline.core :as pipeline]))

(defn- print-exception [error]
  (println 
    (format (str/join 
              "\n"
              ["Error: Pipeline exited on step %s due to exception."
               "Message: %s"
               "(Exception object can be found with pipeline.core/exception)"])
            (:pipeline.step/name error)
            (-> error :details .getMessage)))) 

(defn- print-validation-error [error]
  (println 
    (format "Error: Pipeline exited on step %s due to %s. %s should not have been %s." 
            (:pipeline.step/name error)
            (:reason error)
            (:key error)
            (pr-str ((:key error) pipeline/*pipeline)))))

(defn- print-error [error]
  (if (= (:reason error) :exception)
    (print-exception error)
    (print-validation-error)))

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


