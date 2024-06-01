(ns pipeline.core
  "The core of pipeline. This is where the definition and running
  of the pipelines exists."
  (:require
    [clojure.set :as set]
    [clojure.string :as str]
    [clojure.pprint :refer [pprint print-table]]
    [malli.core :as m]
    [malli.util :as mu]
    [pipeline.default-validations]))

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


;;
;; Malli schemas
;;

(def registry (merge (m/default-schemas) (mu/schemas)))

(def schema-path
  [:or [:sequential any?] any?])

(def schema-paths
  [:sequential schema-path])

(def schema-step-not-started
  [:map
   [:pipeline.step/bindings :map]                ;; keyword / any?
   [:pipeline.step/state [:enum :not-started :successful :failed]]
   [:pipeline.step/seq-id :int]
   [:pipeline.step/name :keyword]
   [:pipeline.step/type [:enum :action :transformation]]
   [:pipeline.step/function any?]                ;; :fn
   [:pipeline.step/input-paths schema-paths]
   [:pipeline.step/output-path schema-path]
   [:pipeline.step/output-schema any?]
   [:pipeline.step/validation-fns :map]])        ;; keyword / ifn?]])

(def schema-step
  [:multi {:dispatch :pipeline.step/state}
   [:not-started schema-step-not-started]
   [:successful schema-step-not-started]
   [:failed schema-step-not-started]])

(def schema-steps
  [:sequential schema-step])

(def schema-pipeline
  [:map
   [:pipeline/steps [:sequential schema-step]]])

(def ^:private step-validator (m/validator schema-step))
(def ^:private steps-validator (m/validator schema-steps))
(def ^:private pipeline-validator (m/validator schema-pipeline))

(def *pipeline "Memory for last pipline executed" nil)


(defn last-run
  "Returns the stored result from the last call to run-pipeline"
  []
  *pipeline)


(defn pipeline?
  "Returns true if given a valid pipeline"
  [pipeline]
  (pipeline-validator pipeline))


(defn step?
  "Returns true if given a valid step"
  [pipeline]
  ;(m/validate schema-step pipeline))    ;; Very notable performance differenece
  (step-validator pipeline))


(defn steps?
  "Returns true is given a vector of steps"
  [pipeline]
  (steps-validator pipeline))


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
  "Returns a named or numbered (seq-id) step in a pipeline"
  [pipeline step-name]
  (->>
    pipeline
    steps
    (filter #(or (= step-name (:pipeline.step/name %))
                 (= step-name (:pipeline.step/seq-id %))))
    first))


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
  ;{:post [(s/valid? ::bindings %)]}

  (cond
    (step? pipeline-or-step)
    (:pipeline.step/bindings pipeline-or-step)

    (pipeline? pipeline-or-step)
    (:pipeline/bindings pipeline-or-step)))


(defn args
  "Returns the args used when run-pipeline was called on this pipeline.
  Will return nil on a pipeline that has not been started"
  [pipeline]
  (:pipeline/args pipeline))


(defn validation-functions
  "Returns the validation functions for a step"
  [step]
  (:pipeline.step/validation-fns step))


(defn state
  "Returns the state of a pipeline or a step."
  [pipeline-or-step]
  ; {:post [(s/valid? ::state %)]}

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


(defn- time-spent [start-time stop-time]
  (/ (double (- stop-time start-time)) 1000000.0))


(defn run-step [f args output-schema state validation-fns options]
  "Runs a single step. This is called from run-pipeline and normally not used directly,
  but is still public since it can be useful during development."

  (let [start-time (System/nanoTime)
        spec (if output-schema (resolve-spec output-schema))
        valid? (get validation-fns :valid?)
        explain (get validation-fns :explain)
        valid-result? #(if spec (valid? state spec %) true)
        trace-fn (:trace-fn options)]

    (when trace-fn
      (trace-fn {:f f :args args}))

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

(defn- get-in-deref
  "Like get-in, but derefs each key recursively in case there is a future."
  ([m ks]
   (get-in-deref m ks nil))

  ([m ks not-found]
   (let [[k & ks] ks
         v (get m k)
         realized-value (if (future? v) (deref v) v)]
     (if (nil? realized-value)
       not-found
       (if (seq ks)
         (get-in-deref realized-value ks not-found)
         realized-value)))))

(defn args-for-step [step state]
  (map
    (fn [input-path]
      (let [path (if (keyword? input-path) [input-path] input-path)]
        (get-in-deref state path)))
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
            (= (:pipeline.step/seq-id st)
               (:pipeline.step/seq-id step))
            i))
        (:pipeline/steps pipeline)))])

(defn update-pipeline [pipeline step]
  (let [path (step-path pipeline step)]
    (assoc-in pipeline path step)))

(defn update-state [state step]
  (let [output-path (:pipeline.step/output-path step)
        path (if (keyword? output-path) [output-path] output-path)]
    (assoc-in state path (:pipeline.step/result step))))

(defn last-executed-step [pipeline]
  (->>
    pipeline
    :pipeline/steps
    reverse
    (drop-while #(= :not-started (:pipeline.step/state %)))
    (first)))

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
   {:pre [(pipeline? pipeline)]
    :post []}

   ;; TODO: validate args

   (let [start-time (System/nanoTime)

         executed-pipeline
         (loop [state (merge (bindings pipeline) args)
                pipeline (assoc pipeline :pipeline/args args)]
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
                  validation-fns (:pipeline.step/validation-fns step)
                  result (run-step f args output-schema state validation-fns options)
                  step' (update-step step args result)
                  pipeline' (update-pipeline pipeline step')
                  state' (update-state state step')]
              (recur state' pipeline'))))

         result-step (last-executed-step executed-pipeline)

         result
         (cond
           (successful? result-step)
           {:pipeline/state           (:pipeline.step/state result-step)
            :pipeline/result          (:pipeline.step/result result-step)}

           (failed? result-step)
           {:pipeline/state           (:pipeline.step/state result-step)
            :pipeline/failure-reason  (:pipeline.step/failure-reason result-step)
            :pipeline/failure-value   (:pipeline.step/failure-value result-step)
            :pipeline/failure-message (:pipeline.step/failure-message result-step)}

           (not-started? result-step)
           {:pipeline/state           (:pipeline.step/state result-step)}

           :else
           {})]

     (->
       executed-pipeline
       (merge result)
       (assoc :pipeline/time-spent (time-spent start-time (System/nanoTime)))))))


(def default-validation-fns pipeline.default-validations/default-validation-fns)

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

(defn make-step
  [step-type step-name f inputs output output-spec bindings validation-fns]
  {:pipeline.step/type step-type
   :pipeline.step/name step-name
   :pipeline.step/function f
   :pipeline.step/input-paths inputs
   :pipeline.step/output-path output
   :pipeline.step/output-schema output-spec
   :pipeline.step/bindings bindings
   :pipeline.step/validation-fns validation-fns
   :pipeline.step/seq-id 0
   :pipeline.step/state :not-started})

(defn action
  ([step-name f inputs]
   (make-step :action step-name f inputs nil nil {} default-validation-fns))
  ([step-name f inputs output]
   (make-step :action step-name f inputs output nil {} default-validation-fns))
  ([step-name f inputs output output-spec]
   (make-step :action step-name f inputs output output-spec {} default-validation-fns))
  ([step-name f inputs output output-spec bindings]
   (make-step :action step-name f inputs output output-spec bindings default-validation-fns))
  ([step-name f inputs output output-spec bindings validation-fns]
   (make-step :action step-name f inputs output output-spec bindings validation-fns)))

(defn transformation
  ([step-name f inputs output]
   (make-step :transformation step-name f inputs output nil {} default-validation-fns))
  ([step-name f inputs output output-spec]
   (make-step :transformation step-name f inputs output output-spec {} default-validation-fns))
  ([step-name f inputs output output-spec bindings]
   (make-step :transformation step-name f inputs output output-spec bindings default-validation-fns))
  ([step-name f inputs output output-spec bindings validation-fns]
   (make-step :transformation step-name f inputs output output-spec bindings validation-fns)))

(defn make-pipeline
  [pipeline-bindings & steps-and-pipelines]
  ;(if-not (s/valid? ::bindings pipeline-bindings)
  ;  (throw (ex-info (with-out-str (s/explain ::bindings pipeline-bindings))
  ;                  {}))]

  (let [steps'
        (reduce
          (fn [sum item]
            (cond
              (step? item)
              (concat sum [item])

              (steps? item)
              (concat sum item)

              (pipeline? item)
              (concat sum (steps item))

              :else
              (throw (ex-info "Not a step, steps or pipeline" {:item item}))))
          []
          steps-and-pipelines)

        ;_ (prn "steps'" steps')

        steps''
        (->>
          steps'
          (map-indexed (fn [i step] (assoc step :pipeline.step/seq-id i)))
          (map (fn [step] (assoc step :pipeline.step/state :not-started)))
          (map (fn [step] (assoc step :pipeline.step/bindings (merge (bindings step) pipeline-bindings))))
          (into []))

        pipeline
        {:pipeline/steps steps''}]

    (if (pipeline? pipeline)
      pipeline
      (throw (ex-info
               "Invalid pipeline."
               {:pipeline pipeline
                :explanation (m/explain schema-pipeline pipeline)})))))

