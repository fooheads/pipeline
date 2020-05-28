(ns pipeline.core
  "The core of pipeline. This is where the definition and running
  of the pipelines exists."
  (:require
    [clojure.string :as str]
    [clojure.pprint :refer [pprint print-table]]
    [malli.core :as m]))

;;
;; Ideas
;;
;; "Print failed call" in the pipeline result? Very useful in logs, application
;; insight and such.
;;


;;;
;;; Schemas
;;;

(def ^:export Step
  (m/schema
    [:map
     [:pipeline.step/name keyword?]
     [:pipeline.step/type [:enum
                           :action
                           :transformation
                           :validation]]
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
              (assoc context :pipeline/error (merge step {:pipeline.error/reason :invalid-output
                                                          ;:key output-path
                                                          :pipeline.error/value result
                                                          :pipeline.error/message (m/explain output-schema result)})))
            context))
        (catch Exception e
          (assoc context :pipeline/error (merge step {:pipeline.error/reason :exception
                                                      :pipeline.error/value e
                                                      :pipeline.error/message (.getMessage e)})))))))

;; Add functions to validate / explain pipeline

(defn run-pipeline
  "Executes a pipeline and returns the full execution context as a result. In the execution context,
  result and errors can be found, but also a full trace of each step's input, output and time spent.

  Options can be given to run-pipeline, but there are not yet any options defined.

  Even though run-pipeline returns the full execution context, it's recommended to use the helper
  functions in this package to extract data for common tasks. This protects you slightly from
  breaking changes in the data structure since this is still pre-alpha.

  The last pipeline result (the full execution context) is stored in pipeline.core/*pipeline as well as returned"

  ([pipeline initial-context] ; TODO: initial-context -> args
   (run-pipeline pipeline initial-context {}))

  ([pipeline initial-context options]
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

(defn get-output  ;; rename to result
  "Returns the output from the last step if the run was successfulr. Always
  returns nil if the run failed.
  If no result is given as an argument, the stored result from the last run is used."
  ([] (get-output (last-result)))
  ([result]
   (when (success? result)
     (:pipeline/last-output result))))

(defn get-error   ;; error
  "Returns the error from pipeline result, and nil if the run was successful.
  If no result is given as an argument, the stored result from the last run is used."
  ([] (get-error (last-result)))
  ([result]
   (when (failure? result)
     (:pipeline/error result))))

(defn exception?
  "Predicate that returns true if the pipeline run resulted in an exception.
  If no result is given as an argument, the stored result from the last run is used."
  ([] (exception? (last-result)))
  ([result]
   (= (-> result get-error :pipeline.error/reason) :exception)))

(defn get-exception   ;; exception
  "Returns the exception from pipeline result, and nil if the pipeline did not end
  with an exception.  If no result is given as an argument, the stored result from
  the last run is used."
  ([] (get-exception (last-result)))
  ([result]
   (when (and (failure? result)
              (exception? result))
     (:pipeline.error/value result))))

(comment
  (ns pipeline.core)
  (last-result)
  (print-result)
  (boolean (:pipeline/error (last-result)))
  (failure? (last-result))
  (pipeline.core/print-problematic-call)

  (clojure.test/run-tests 'pipeline.core-test)

  (def context (get-initial-context *pipeline))

  (defn run-step! [context f input output])

  (->
   [(action fjdbc/execute [:data-source :relation-query-work-orders] :work-orders)
    (transformation normalize [:work-orders :normalization-spec])]
   (pipeline/run! initial))



  :get-tenant | "....."
  :wor        | 34

  (run-pipeline-to! pipeline 3)

  (my-next-fn (:foo context) (:bar context)))

(comment
  (defn normalize [normalization-spec set-of-trees])

  {:steps [{:name :normalize
            :function #'normalize
            :binding {:normalization-spec [[:AppointmentDetails/AppointmentDetailsID] ,,,]}
            :input-paths [[:normalization-spec] [:result-trees]]
            :output-path :normalized-trees}]
   :binding {:spm-auth-key "sdfjhkjlh12322"}
   :params {:data-source #'data-source?
            :spm-url #'url?}
   :repl {:datasource "@repl/ds"}})



