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

(s/def ::bindings (s/map-of keyword? any?))
(s/def ::validation-functions (s/map-of keyword? ifn?))

;; Things that exist before a step is executed.

(s/def ::state #{:not-started :successful :failed})

(s/def :pipeline.step/bindings ::bindings)
(s/def :pipeline.step/state ::state)

(s/def :pipeline.step/seq-id integer?)

(s/def :pipeline.step/name keyword?)
(s/def :pipeline.step/type #{:action :transformation :validation})
(s/def :pipeline.step/function ifn?)

(s/def ::path (s/or :key keyword? :path (s/coll-of keyword?)))
(s/def :pipeline.step/input-path ::path)
(s/def :pipeline.step/input-paths (s/coll-of :pipeline.step/input-path))

(s/def :pipeline.step/output-path ::path)
(s/def :pipeline.step/output-schema any?)   ;; spec

;; Things that are added when a step can be / is executed.
(s/def :pipeline.step/args (s/coll-of any?))

;; Things that are added when a step has been executed successfully.
(s/def :pipeline.step/result any?)
(s/def :pipeline.step/time-spent double?)

;; Things that are added when a step has been executed and failed.
(s/def :pipeline.step/failure-reason #{:invalid-output :exception})
(s/def :pipeline.step/failure-value any?)
(s/def :pipeline.step/failure-message any?)

(defmulti pipeline-step :pipeline.step/state)

(defmethod pipeline-step :not-started [_]
  (s/keys :req [:pipeline.step/bindings
                :pipeline.step/state
                :pipeline.step/seq-id
                :pipeline.step/name
                :pipeline.step/type
                :pipeline.step/function
                :pipeline.step/input-paths
                :pipeline.step/output-path
                :pipeline.step/output-schema]))

(defmethod pipeline-step :successful [_]
  (s/keys :req [:pipeline.step/bindings
                :pipeline.step/state
                :pipeline.step/seq-id
                :pipeline.step/name
                :pipeline.step/type
                :pipeline.step/function
                :pipeline.step/input-paths
                :pipeline.step/output-path
                :pipeline.step/output-schema
                :pipeline.step/result
                :pipeline.step/time-spent]))

(defmethod pipeline-step :failed [_]
  (s/keys :req [:pipeline.step/bindings
                :pipeline.step/state
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

(s/def :pipeline/state ::state)
(s/def :pipeline/step (s/multi-spec pipeline-step :pipeline.step/state))
(s/def :pipeline/steps (s/coll-of :pipeline/step))
(s/def :pipeline/bindings ::bindings)
(s/def :pipeline/validation-functions ::validation-functions)

(s/def :pipeline/pipeline
  (s/keys :req [:pipeline/steps :pipeline/bindings :pipeline/validation-functions]))

(def *pipeline nil)

(defn last-run
  "Returns the stored result from the last call to run-pipeline"
  []
  *pipeline)

(defn pipeline?
  "Returns true if given a valid pipeline"
  [pipeline]
  (s/valid? :pipeline/pipeline pipeline))

(defn step?
  "Returns true if given a valid step"
  [pipeline]
  (s/valid? :pipeline/step pipeline))

(defn- kind [pipeline-or-step]
  (cond
    (pipeline? pipeline-or-step) :pipeline
    (step? pipeline-or-step) :step
    :else :unknown))

(defn steps
  "Returns the steps in a pipeline"
  [pipeline]
  (:pipeline/steps pipeline))

(defn step
  "Returns a named step in a pipeline"
  [pipeline step-name]
  (->> pipeline steps (filter #(= step-name (:pipeline.step/name %))) first))

(defn step-name
  "Returns the name of a step"
  [step]
  (:pipeline.step/name step))

(declare not-started?)
(declare successful?)
(declare failed?)

(defn bindings
  "Returns the bindings of a pipeline or a step."
  [pipeline-or-step]
  {:post [(s/valid? ::bindings %)]}

  (cond
    (step? pipeline-or-step)
    (:pipeline.step/bindings pipeline-or-step)

    (pipeline? pipeline-or-step)
    (:pipeline/bindings pipeline-or-step)))

(defn validation-functions
  "Returns the validation functions for  a pipeline"
  [pipeline]
  {:post [(s/valid? ::validation-functions %)]}

  (:pipeline/validation-functions pipeline))

(defn state
  "Returns the state of a pipeline or a step."
  [pipeline-or-step]
  {:post [(s/valid? ::state %)]}

  (cond
    (step? pipeline-or-step)
    (:pipeline.step/state pipeline-or-step)

    (pipeline? pipeline-or-step)
    (let [steps (steps pipeline-or-step)]
      (cond
        (every? not-started? steps) :not-started
        (every? successful? steps) :successful
        (some failed? steps) :failed
        :else (throw (ex-info "Unknown state in pipeline!" {:pipeline pipeline-or-step}))))))

(defn state? [state-value pipeline-or-step]
  "Returns the state for a pipeline or a step"
  (boolean (= state-value (state pipeline-or-step))))

(defn not-started?
  "Returns true if the pipeline or a step or not started."
  [pipeline-or-step]
  (state? :not-started pipeline-or-step))

(defn successful?
  "Returns true if the pipeline or a step was successful."
  [pipeline-or-step]
  (state? :successful pipeline-or-step))

(defn failed?
  "Returns true if the pipeline or a step was failed."
  [pipeline-or-step]
  (state? :failed pipeline-or-step))

(defn failed-steps
  "Returns all failed steps of a pipeline."
  [pipeline]
  (filter failed? (steps pipeline)))

(defn failed-step
  "Returns the first failed step of a pipeline."
  [pipeline]
  (first (failed-steps pipeline)))

(defn failure-reason [step]
  (:pipeline.step/failure-reason step))

(defn failure-value [step]
  (:pipeline.step/failure-value step))

(defn failure-message [step]
  (:pipeline.step/failure-message step))

#_(defn failed-calls
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

(defn result
  "Returns the result from a successful step or from a successful pipeline.
  If the arg is a pipeline, the result is the result of the last step.

  If called on a step that is not successful or a pipeline that is not successful,
  an exception is thrown."
  [pipeline-or-step]
  (if (pipeline? pipeline-or-step)
    (result (last (steps pipeline-or-step))))

  (if-not
    (and
      (or (pipeline? pipeline-or-step)
          (step? pipeline-or-step))
      (successful? pipeline-or-step))
    (throw (ex-info "result can only be retreived from a successful pipeline or step"
                    {:value pipeline-or-step})))

  (cond
    (step? pipeline-or-step)
    (:pipeline.step/result pipeline-or-step)

    (pipeline? pipeline-or-step)
    (result (last (steps pipeline-or-step)))))
;;
;; New take
;;

(defn run-step [f args output-schema state validation-functions]
  "Runs a single step. This is called from run-pipeline and normally not used directly,
  but is still public since it can be useful during development."

  (let [time-spent (fn [start-time stop-time]
                     (/ (double (- stop-time start-time)) 1000000.0))
        start-time (System/nanoTime)
        spec (if output-schema (resolve-spec output-schema))
        valid? (get validation-functions :valid?)
        explain (get validation-functions :explain)
        valid-result? #(if spec (valid? state spec %) %)]

    (try
     (let [result (apply f args)]
       (if (valid-result? result)
         {:pipeline.step/time-spent (time-spent start-time (System/nanoTime))
          :pipeline.step/state :successful
          :pipeline.step/result result}

         {:pipeline.step/time-spent (time-spent start-time (System/nanoTime))
          :pipeline.step/state :failed
          :pipeline.step/failure-reason :invalid-output
          :pipeline.step/failure-value result
          :pipeline.step/failure-message (explain state spec result)}))

     (catch Exception e
       {:pipeline.step/time-spent (time-spent start-time (System/nanoTime))
        :pipeline.step/state :failed
        :pipeline.step/failure-reason :exception
        :pipeline.step/failure-value e
        :pipeline.step/failure-message (.getMessage e)}))))



(defn args-for-step [step state]
  (map
    (fn [input-path]
      (let [path (if (keyword? input-path) [input-path] input-path)]
        (get-in state path)))
    (:pipeline.step/input-paths step)))

(defn pipeline-finished? [pipeline]
  (or
    (every? #(= :successful (:pipeline.step/state %)) (:pipeline/steps pipeline))
    (boolean (some #(= :failed (:pipeline.step/state %)) (:pipeline/steps pipeline)))))

(defn next-step [pipeline]
  (some #(when (= :not-started (:pipeline.step/state %)) %) (:pipeline/steps pipeline)))

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

   (loop [state (merge (bindings pipeline) args)
          pipeline pipeline]
     (if (pipeline-finished? pipeline)
       (do
         (def *pipeline pipeline)
         pipeline)
       (let [step (next-step pipeline)
             bindings (bindings step)
             state (merge state bindings)
             f (:pipeline.step/function step)
             args (args-for-step step state)
             output-schema (:pipeline.step/output-schema step)
             result (run-step f args output-schema state (validation-functions pipeline))
             step' (update-step step args result)
             pipeline' (update-pipeline pipeline step')
             state' (update-state state step')]
         (recur state' pipeline'))))))

;;
;; precedence nu
;; 1. step
;; 2. args
;; 3. pipeline

;;
;; precedence nu
;; 1. step
;; 2. pipeline
;; 3. args

;;
;; precedence nu
;; 1. args
;; 2. pipeline
;; 3. step

;;;
;;; Helper functions
;;;

(defn action
  ([step-name f inputs]
   (action step-name f inputs nil nil {}))
  ([step-name f inputs output]
   (action step-name f inputs output nil {}))
  ([step-name f inputs output output-spec]
   (action step-name f inputs output output-spec {}))
  ([step-name f inputs output output-spec bindings]
   {:pipeline.step/bindings bindings
    :pipeline.step/name step-name
    :pipeline.step/type :action
    :pipeline.step/function f
    :pipeline.step/input-paths inputs
    :pipeline.step/output-path output
    :pipeline.step/output-schema output-spec}))

(defn transformation
  ([step-name f inputs output]
   (action step-name f inputs output nil {}))
  ([step-name f inputs output output-spec]
   (action step-name f inputs output output-spec {}))
  ([step-name f inputs output output-spec bindings]
   {:pipeline.step/bindings bindings
    :pipeline.step/name step-name
    :pipeline.step/type :transformation
    :pipeline.step/function f
    :pipeline.step/input-paths inputs
    :pipeline.step/output-path output
    :pipeline.step/output-schema output-spec}))


(defn valid? [context spec v]
  (s/valid? spec v))

(defn explain-data [context spec v]
  (s/explain-data spec v))

(def default-validation-functions
  {:valid? valid?
   :explain explain-data})

(defn make-pipeline
  ([steps]
   (make-pipeline steps {} default-validation-functions))
  ([steps bindings]
   (make-pipeline steps bindings default-validation-functions))
  ([steps bindings validation-functions]
   {:post [(s/valid? :pipeline/pipeline %)]}

   {:pipeline/steps (->>
                      steps
                      (map-indexed (fn [i step] (assoc step :pipeline.step/seq-id i)))
                      (map (fn [step] (assoc step :pipeline.step/state :not-started)))
                      (into []))
    :pipeline/bindings bindings
    :pipeline/validation-functions validation-functions}))


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




