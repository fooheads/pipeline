(ns pipeline.core
  "The core of pipeline. This is where the definition and running 
  of the pipelines exists."
  (:require
    [clojure.string :as str]
    [clojure.pprint :refer [pprint print-table]]
    [malli.core :as m]))

;;;
;;; Schemas
;;; 

(def ^:export Step
  (m/schema
    [:map
     [:pipeline.step/name keyword?]
     [:pipeline.step/type [:enum :action :transformation :validation]] 
     [:pipeline.step/function any?]
     [:pipeline.step/input-paths sequential?]
     [:pipeline.step/output-path {:optional true} keyword?]
     [:pipeline.step/output-schema {:optional true} any?]]))
    
(def ^:export Pipeline
  (m/schema
    [:sequential Step]))

;;;
;;; private
;;;

(defn- append-trace [trace step result time-spent]
  (concat trace [(merge step {:pipeline.step-result/value result
                              :pipeline.step-result/time-spent time-spent})]))

;;;
;;; public
;;;


(defn run-step [context step options]
  "Runs a single step. This is called from run-pipeline and normally not used directly,
  but is still public since it can be useful during development."

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
              context (update context :pipeline/trace append-trace step result time-spent)]

          (if output-schema
            (if (m/validate output-schema result)
              context
              (assoc context :pipeline/error (merge step {:reason :invalid-output
                                                          :key output-path
                                                          :details (m/explain output-schema result)})))
            context))
        (catch Exception e
          (assoc context :pipeline/error (merge step {:reason :exception
                                                      :details e})))))))
          

(defn run-pipeline
  "Executes a pipeline and returns the full execution context as a result. In the execution context,
  result and errors can be found, but also a full trace of each step's input, output and time spent.
  
  Options can be given to run-pipeline, but there are not yet any options defined.
  
  Even though run-pipeline returns the full execution context, it's recommended to use the helper
  functions in this package to extract data for common tasks. This protects you slightly from
  breaking changes in the data structure since this is still pre-alpha.
  
  The last pipeline result (the full execution context) is stored in pipeline.core/*pipeline as well as returned"

  ([initial-context pipeline]
   (run-pipeline initial-context pipeline {}))
  
  ([initial-context pipeline options]
   (assert (m/validate Pipeline pipeline) "Not a valid pipeline!")
   (let [result (reduce (fn [context step] (run-step context step options)) initial-context pipeline)]
     (def *pipeline result)
     result)))

;;;
;;; Helper functions
;;;

(defn last-result 
  "Returns the stored result from the last call to run-pipeline" 
  []
  *pipeline)

(defn success? 
  "Predicate that returns true if the pipeline result was a success. If no
  result is given as an argument, the stored result from the last run is used."
  ([] (success? (last-result)))
  ([result]
   (not (:pipeline/error result))))

(defn failure?
  "Predicate that returns true if the pipeline run resulted in an error. 
  If no result is given as an argument, the stored result from the last run is used."
  ([] (failure? (last-result)))
  ([result]
   (not (success? result))))

(defn get-output
  "Returns the output from the last step if the run was successfulr. Always 
  returns nil if the run failed.
  If no result is given as an argument, the stored result from the last run is used."
  ([] (get-output (last-result)))
  ([result]
   (when (success? result)
     (:pipeline/last-output result))))

(defn get-error
  "Returns the error from pipeline result, and nil if the run was successful.
  If no result is given as an argument, the stored result from the last run is used."
  ([] (get-error (last-result)))
  ([result]
   (when (failure? result)
     (:pipeline/error result))))


(comment
  (ns pipeline.core)
  (last-result)
  (print-result)
  (boolean (:pipeline/error (last-result)))
  (failure? (last-result))
  (pipeline.core/print-problematic-call)

  (clojure.test/run-tests 'pipeline.core-test))

