(ns pipeline.core
  "The core of pipeline. This is where the definition and running
  of the pipelines exists."
  (:require
    [clojure.spec.alpha :as s]
    [clojure.string :as str]
    [clojure.pprint :refer [pprint print-table]]))
    ;[malli.core :as m]))

;;
;; Ideas
;;
;; "Print failed call" in the pipeline result? Very useful in logs, application
;; insight and such.
;;


;;;
;;; Schemas
;;;

(s/def :pipeline.step/name keyword?)
(s/def :pipeline.step/type #{:action :transformation :validation})
(s/def :pipeline.step/function any?)
(s/def :pipeline.step/input-paths sequential?)
(s/def :pipeline.step/output-path keyword?)
(s/def :pipeline.step/output-schema any?)

(s/def :pipeline/step
  (s/keys :req [:pipeline.step/name
                :pipeline.step/type
                :pipeline.step/function
                :pipeline.step/input-paths]
          :opt [:pipeline.step/output-path
                :pipeline.step/output-schema]))

(s/def :pipeline/pipeline
  (s/coll-of :pipeline/step))

(defn valid? [pipeline]
  (s/valid? :pipeline/pipeline pipeline))

(defn explain [pipeline]
  (s/explain :pipeline/pipeline pipeline))

;;;
;;; private
;;;

(defn- step-execution-success [step args result time-spent]
  (merge
    (select-keys step [:pipeline.step/name])
    {:pipeline.step-execution/args args
     :pipeline.step-execution/result result
     :pipeline.step-execution/time-spent time-spent}))

(defn- step-execution-error [step args reason value message time-spent]
  (merge
    (select-keys step [:pipeline.step/name])
    {:pipeline.step-execution/args args
     :pipeline.step-execution/time-spent time-spent}
    {:pipeline.error/reason reason
     :pipeline.error/value value
     :pipeline.error/message message}))

;;;
;;; public
;;;


;;
;;
;;
;; ======================================================
;; TODO: Make thing work with state under :pipeline/state
;; ======================================================
;;
;;
;;

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
          args (map
                 (fn [input-path]
                   (let [path (cons :pipeline/state input-path)]
                     (get-in context path)))
                 input-paths)]
      (try
        (let [start-time (System/nanoTime)
              result (apply f args)
              stop-time (System/nanoTime)
              time-spent (/ (double (- stop-time start-time)) 1000000.0)
              context (assoc-in context [:pipeline/state output-path] result)
              context (update context :pipeline/step-executions conj (step-execution-success step args result time-spent))]

          (if output-schema
            (if (s/valid? output-schema result)
              context
              (let [reason :invalid-output
                    value result
                    message (s/explain output-schema value)]
                (->
                  context
                  (update :pipeline/step-executions conj (step-execution-error step args reason value message time-spent))
                  ;; HERE - add error to step-executions
                  (assoc :pipeline/error (merge step {:pipeline.error/reason reason
                                                      :pipeline.error/value value
                                                      :pipeline.error/message message})))))
            context))
        (catch Exception e
          (let [reason :exception
                value e
                message (.getMessage e)]
            (->
              context
              (update :pipeline/step-executions conj (step-execution-error step args reason value message nil))
              (assoc :pipeline/error (merge step {:pipeline.error/reason :exception
                                                  :pipeline.error/value value
                                                  :pipeline.error/message message})))))))))

;; Add functions to validate / explain pipeline

(defn run-pipeline
  "Executes a pipeline and returns the full execution context as a result. In the execution context,
  result and errors can be found, but also a full trace of each step's input, result and time spent.

  Options can be given to run-pipeline, but there are not yet any options defined.

  Even though run-pipeline returns the full execution context, it's recommended to use the helper
  functions in this package to extract data for common tasks. This protects you slightly from
  breaking changes in the data structure since this is still pre-alpha.

  The last pipeline result (the full execution context) is stored in pipeline.core/*pipeline as well as returned"

  ([pipeline args]
   (run-pipeline pipeline args {}))

  ([pipeline args options]
   (assert (valid? pipeline) "Not a valid pipeline!")

   (let [context {:pipeline/steps pipeline :pipeline/step-executions [] :pipeline/state args}
         result (reduce (fn [context step] (run-step context step options)) context pipeline)]
     (def *pipeline result)
     result)))

;;;
;;; Helper functions
;;;

(defn last-run
  "Returns the stored result from the last call to run-pipeline"
  []
  *pipeline)

(defn success?
  "Predicate that returns true if the pipeline result was a success. If no
  result is given as an argument, the stored result from the last run is used."
  ([] (success? (last-run)))
  ([result]
   (not (:pipeline/error result))))

(defn failure?
  "Predicate that returns true if the pipeline run resulted in an error.
  If no result is given as an argument, the stored result from the last run is used."
  ([] (failure? (last-run)))
  ([result]
   (not (success? result))))

(defn get-output  ;; rename to result
  "Returns the output from the last step if the run was successfulr. Always
  returns nil if the run failed.
  If no result is given as an argument, the stored result from the last run is used."
  ([] (get-output (last-run)))
  ([result]
   (when (success? result)
     (some->> result :pipeline/step-executions last :pipeline.step-execution/result))))

(defn get-error   ;; error
  "Returns the error from pipeline result, and nil if the run was successful.
  If no result is given as an argument, the stored result from the last run is used."
  ([] (get-error (last-run)))
  ([result]
   (when (failure? result)
     (:pipeline/error result))))

(defn exception?
  "Predicate that returns true if the pipeline run resulted in an exception.
  If no result is given as an argument, the stored result from the last run is used."
  ([] (exception? (last-run)))
  ([result]
   (= (-> result get-error :pipeline.error/reason) :exception)))

(defn get-exception   ;; exception
  "Returns the exception from pipeline result, and nil if the pipeline did not end
  with an exception.  If no result is given as an argument, the stored result from
  the last run is used."
  ([] (get-exception (last-run)))
  ([result]
   (when (and (failure? result)
              (exception? result))
     (some->> result :pipeline/step-executions last :pipeline.error/value))))

(defn action
  ([step-name f inputs]
   (action step-name f inputs nil nil))
  ([step-name f inputs output]
   (action step-name f inputs output nil))
  ([step-name f inputs output output-spec]
   {:pipeline.step/name step-name
    :pipeline.step/type :action
    :pipeline.step/function f
    :pipeline.step/input-paths inputs
    :pipeline.step/output-path output
    :pipeline.step/output-schema output-spec}))

(defn transformation
  ([step-name f inputs output]
   (action step-name f inputs output nil))
  ([step-name f inputs output output-spec]
   {:pipeline.step/name step-name
    :pipeline.step/type :transformation
    :pipeline.step/function f
    :pipeline.step/input-paths inputs
    :pipeline.step/output-path output
    :pipeline.step/output-schema output-spec}))

(comment
  (ns pipeline.core)
  (last-run)
  (print-result)
  (boolean (:pipeline/error (last-run)))
  (failure? (last-run))
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



