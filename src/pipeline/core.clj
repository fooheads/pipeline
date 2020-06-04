(ns pipeline.core
  "The core of pipeline. This is where the definition and running
  of the pipelines exists."
  (:require
    [clojure.set :as set]
    [clojure.spec.alpha :as s]
    [clojure.string :as str]
    [clojure.pprint :refer [pprint print-table]]))

(defn resolve-spec [var-or-any]
  (if (var? var-or-any)
    (var-get var-or-any)
    var-or-any))

;;
;; Ideas
;;
;; "Print failed call" in the pipeline result? Very useful in logs, application
;; insight and such.
;;


;;;
;;; Schemas
;;;

(s/def :pipeline/bindings (s/map-of keyword? any?))

;; Things that exist before a step is executed.

(s/def :pipeline.step/state #{:not-run :success :failure})
(s/def :pipeline.step/seq-id integer?)

(s/def :pipeline.step/name keyword?)
(s/def :pipeline.step/type #{:action :transformation :validation})
(s/def :pipeline.step/function ifn?)

(s/def :pipeline.step/input-path (s/or :key keyword? :path (s/coll-of keyword?)))
(s/def :pipeline.step/input-paths (s/coll-of :pipeline.step/input-path))

(s/def :pipeline.step/output-path keyword?)
(s/def :pipeline.step/output-schema any?)

;; Things that are added when a step can be / is executed.
(s/def :pipeline.step/args (s/coll-of any?))

;; Things that are added when a step has been executed successfully.
(s/def :pipeline.step/result any?)
(s/def :pipeline.step/time-spent double?)

;; Things that are added when a step has been executed and failed.
(s/def :pipeline.step/failure-reason #{:invalid-output :exception})
(s/def :pipeline.step/failure-value any?)
(s/def :pipeline.step/failure-message any?)

; (s/def :pipeline.step.failure/reason #{:invalid-output :exception})
; (s/def :pipeline.step.failure/value any?)
; (s/def :pipeline.step.failure/message any?)
;
; (s/def :pipeline.step/failure
;   (s/keys :req-un [:pipeline.step.failure/reason
;                    :pipeline.step.failure/value
;                    :pipeline.step.failure/message]))
;


(defmulti pipeline-step :pipeline.step/state)

(defmethod pipeline-step :not-run [_]
  (s/keys :req [:pipeline.step/state
                :pipeline.step/seq-id
                :pipeline.step/name
                :pipeline.step/type
                :pipeline.step/function
                :pipeline.step/input-paths
                :pipeline.step/output-path
                :pipeline.step/output-schema]))

(defmethod pipeline-step :success [_]
  (s/keys :req [:pipeline.step/state
                :pipeline.step/seq-id
                :pipeline.step/name
                :pipeline.step/type
                :pipeline.step/function
                :pipeline.step/input-paths
                :pipeline.step/output-path
                :pipeline.step/output-schema
                :pipeline.step/result
                :pipeline.step/time-spent]))

(defmethod pipeline-step :failure [_]
  (s/keys :req [:pipeline.step/state
                :pipeline.step/seq-id
                :pipeline.step/name
                :pipeline.step/type
                :pipeline.step/function
                :pipeline.step/input-paths
                :pipeline.step/output-path
                :pipeline.step/output-schema
                :pipeline.step/failure-reason
                :pipeline.step/failure-value
                :pipeline.step/failure-message]))

(s/def :pipeline/step (s/multi-spec pipeline-step :pipeline.step/state))
(s/def :pipeline/steps (s/coll-of :pipeline/step))

(s/def :pipeline/pipeline
  (s/keys :req [:pipeline/steps :pipeline/bindings]))

;;;
;;; private
;;;

(defn- step-run-success [step args result time-spent]
  (merge
    (select-keys step [:pipeline.step/name])
    {:pipeline.step-run/args args
     :pipeline.step-run/result result
     :pipeline.step-run/time-spent time-spent}))

(defn- step-run-error [step args reason value message time-spent]
  (merge
    (select-keys step [:pipeline.step/name])
    {:pipeline.step-run/args args
     :pipeline.step-run/time-spent time-spent}
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

(def *pipeline nil)

(defn last-run
  "Returns the stored result from the last call to run-pipeline"
  []
  *pipeline)

(defn- steps [pipeline]
  (:pipeline/steps pipeline))

(defn pipeline
  ([] (pipeline (last-run)))
  ([run]
   (:pipeline/pipeline run)))

(defn step
  ([run step-name]
   (->> run pipeline steps (filter #(= step-name (:pipeline.step/name %))) first)))

(defn step-runs
  ([] (step-runs (last-run)))
  ([run]
   (:pipeline/step-runs run)))

(defn step-runs'
  ([] (step-runs' (last-run)))
  ([run]
   (map
     (fn [step-run] (merge step-run (step run (:pipeline.step/name step-run))))
     (step-runs run))))

(defn failed-step? [step]
  (boolean (:pipeline.error/reason step)))

(defn failed-steps
  ([] (failed-steps (last-run)))
  ([run]
   (set (filter failed-step? (step-runs run)))))


(defn failed-calls
  ([] (failed-calls (last-run)))
  ([run]
   (let [failed-steps (failed-steps run)
         steps (set (get-in run [:pipeline/pipeline :pipeline/steps]))
         foos (set/join failed-steps steps)]

     (map
       (fn [foo]
         (concat
           (list (:pipeline.step/function foo))
           (:pipeline.step-run/args foo)))
       foos))))

(defn errors
  ([] (errors (last-run)))
  ([run]
   (->>
     run
     (failed-steps)
     (map #(select-keys % [:pipeline.error/message
                           :pipeline.error/reason
                           :pipeline.error/value])))))

(def failed-call (comp first failed-calls))
(def failed-step (comp first failed-steps))
(def error (comp first errors))

(defn success?
  "Predicate that returns true if the pipeline result was a success. If no
  result is given as an argument, the stored result from the last run is used."
  ([] (success? (last-run)))
  ([run]
   (empty? (failed-calls run))))

(defn failure?
  "Predicate that returns true if the pipeline run resulted in an error.
  If no result is given as an argument, the stored result from the last run is used."
  ([] (failure? (last-run)))
  ([run]
   (not (success? run))))

(defn exception?
  "Predicate that returns true if the pipeline run resulted in an exception.
  If no result is given as an argument, the stored result from the last run is used."
  ([] (exception? (last-run)))
  ([run]
   (= (-> run error :pipeline.error/reason) :exception)))

(defn get-exception   ;; exception
  "Returns the exception from pipeline result, and nil if the pipeline did not end
  with an exception.  If no result is given as an argument, the stored result from
  the last run is used."
  ([] (get-exception (last-run)))
  ([result]
   (when (and (failure? result)
              (exception? result))
     (some->> result :pipeline/step-runs last :pipeline.error/value))))

(defn result
  "Returns the output from the last step if the run was successfulr. Always
  returns nil if the run failed.
  If no result is given as an argument, the stored result from the last run is used."
  ([] (result (last-run)))
  ([run]
   (if (success? run)
     (some->> run :pipeline/step-runs last :pipeline.step-run/result)
     :pipeline/failure)))

;;
;; New take
;;

(defn run-step [f args output-schema]
  "Runs a single step. This is called from run-pipeline and normally not used directly,
  but is still public since it can be useful during development."

  (let [time-spent (fn [start-time stop-time]
                     (/ (double (- stop-time start-time)) 1000000.0))
        start-time (System/nanoTime)
        spec (if output-schema (resolve-spec output-schema))
        valid-result? #(if spec (s/valid? spec %) %)
        explain-invalid-output #(:clojure.spec.alpha/problems (s/explain-data spec %))]

    (try
     (let [result (apply f args)]
       (if (valid-result? result)
         {:pipeline.step/time-spent (time-spent start-time (System/nanoTime))
          :pipeline.step/state :success
          :pipeline.step/result result}

         {:pipeline.step/time-spent (time-spent start-time (System/nanoTime))
          :pipeline.step/state :failure
          :pipeline.step/failure-reason :invalid-output
          :pipeline.step/failure-value result
          :pipeline.step/failure-message (:clojure.spec.alpha/problems
                                           (explain-invalid-output result))}))

     (catch Exception e
       {:pipeline.step/time-spent (time-spent start-time (System/nanoTime))
        :pipeline.step/state :failure
        :pipeline.step/failure-reason :exception
        :pipeline.step/failure-value e
        :pipeline.step/failure-message (.getMessage e)}))))



(defn args-for-step [step state]
  (map
    (fn [input-path]
      (get-in state input-path))
    (:pipeline.step/input-paths step)))

(defn pipeline-finished? [pipeline]
  (or
    (every? #(= :success (:pipeline.step/state %)) (:pipeline/steps pipeline))
    (boolean (some #(= :failure (:pipeline.step/state %)) (:pipeline/steps pipeline)))))

(defn next-step [pipeline]
  (some #(when (= :not-run (:pipeline.step/state %)) %) (:pipeline/steps pipeline)))

(defn update-step [step args result]
  (merge step {:pipeline.step/args (into [] args)} result))

(defn step-path [pipeline step]
  [:pipeline/steps
    (first
      (keep-indexed
        (fn [i st]
          (when
            (= (:pipeline.step/name st)
               (:pipeline.step/name step))
            i))
        (:pipeline/steps pipeline)))])

(defn update-pipeline [pipeline step]
  (let [path (step-path pipeline step)]
    (assoc-in pipeline path step)))

(defn update-state [state step]
  (assoc state (:pipeline.step/output-path step) (:pipeline.step/result step)))

(defn run-pipeline
  "Executes a pipeline and returns the full execution context as a result. In the execution context,
  result and errors can be found, but also a full trace of each step's input, result and time spent.

  Options can be given to run-pipeline, but there are not yet any options defined.

  Even though run-pipeline returns the full execution context, it's recommended to use the helper
  functions in this package to extract data for common tasks. This protects you slightly from
  breaking changes in the data structure since this is still pre-alpha.

  The last pipeline result (the full execution context) is stored in pipeline.core/*pipeline-run as well as returned"

  ([pipeline args]
   (run-pipeline pipeline args {}))

  ([pipeline args options]
   ;; TODO: Validate that the pipeline is not-run
   {:pre [(s/valid? :pipeline/pipeline pipeline)]
    :post []}

   ;; TODO: validate args

   (loop [state (merge (:binding pipeline) args)
          pipeline pipeline]
     (if (pipeline-finished? pipeline)
       (do
         (def *pipeline pipeline)
         pipeline)
       (let [step (next-step pipeline)
             f (:pipeline.step/function step)
             args (args-for-step step state)
             output-schema (:pipeline.step/output-schema step)
             result (run-step f args output-schema)
             step' (update-step step args result)
             pipeline' (update-pipeline pipeline step')
             state' (update-state state step')]
         (recur state' pipeline'))))))


;;;
;;; Helper functions
;;;

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

(defn make-pipeline
  ([steps]
   (make-pipeline steps {}))
  ([steps bindings]
   {};; :pre [(s/valid? :pipeline/step-definitions steps)]
    ;;:post [(s/valid? :pipeline/pipeline %)]}

   {:pipeline/steps (->>
                      steps
                      (map-indexed (fn [i step] (assoc step :pipeline.step/seq-id i)))
                      (map (fn [step] (assoc step :pipeline.step/state :not-run)))
                      (into []))
    :pipeline/bindings bindings}))


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



