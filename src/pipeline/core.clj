(ns pipeline.core
  (:require
    [clojure.pprint :refer [pprint print-table]]
    [malli.core :as m]))

(def ^:export Step
  (m/schema
    [:map
     [:pipeline.step/name keyword?]
     [:pipeline.step/type [:enum :action :transformation :validation]] 
     [:pipeline.step/function any?]
     [:pipeline.step/input-paths sequential?]
     [:pipeline.step/output-path keyword?]
     [:pipeline.step/output-schema {:optional true} any?]]))
    
(def ^:export Pipeline
  (m/schema
    [:sequential Step]))

(defn- append-trace [trace step-name output-path result time-spent]
  (concat trace [{:pipeline.step/name step-name
                  :key output-path 
                  :value result
                  :pipeline.step/time-spent time-spent}]))

(defn run-step [context step options]
  (if (:pipeline/error context)
    (reduced context)
    (let [step-name (:pipeline.step/name step)
          input-paths (:pipeline.step/input-paths step)
          output-path (:pipeline.step/output-path step) 
          output-schema (:pipeline.step/output-schema step)
          f (:pipeline.step/function step)
          args (map #(get-in context %) input-paths)]
      (try
        (let [start-time (System/nanoTime)
              result (apply f args)
              stop-time (System/nanoTime)
              time-spent (/ (double (- stop-time start-time)) 1000000.0)
              context (assoc context output-path result)
              context (assoc context :pipeline/last-output result)
              context (update context :pipeline/trace append-trace step-name output-path result time-spent)]

          (if output-schema
            (if (m/validate output-schema result)
              context
              (assoc context :pipeline/error {:pipeline.step/name step-name
                                              :reason :invalid-output
                                              :key output-path
                                              :details (m/explain output-schema result)}))
            context))
        (catch Exception e
          (assoc context :pipeline/error {:pipeline.step/name step-name
                                          :reason :exception
                                          :details e}))))))
          

(defn run-pipeline
  ([initial-context pipeline]
   (run-pipeline initial-context pipeline {}))
  
  ([initial-context pipeline options]
   (assert (m/validate Pipeline pipeline) "Not a valid pipeline!")
   (let [result (reduce (fn [context step] (run-step context step options)) initial-context pipeline)]
     (def *pipeline result)
     result)))

(defn last-result []
  *pipeline)

(defn print-result []
  (if-let [error (:pipeline/error *pipeline)]
    (println 
      (format "Error: Pipeline exited on step %s due to %s. %s should not have been %s." 
              (:pipeline.step/name error)
              (:reason error)
              (:key error)
              (pr-str ((:key error) *pipeline))))
    (println (format "Success"))))


