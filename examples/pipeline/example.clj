(ns pipeline.example
  (:require
    [clojure.spec.alpha :as s]
    [pipeline.core :as pipeline]
    [pipeline.print]))

;;
;; Guitarist spec
;;

(s/def :guitarist/name string?)
(s/def :guitarist/born integer?)
(s/def :guitarist/died integer?)

(s/def :guitarist/guitarist (s/keys :req-un [:guitarist/name :guitarist/born]
                                    :opt-un [:guitarist/died]))

;;
;; The functions
;;

(defn get-guitarist [guitarist-id]
  (get
    {:jimi  {:name "Jimi Hendrix" :born 1942 :died 1970}
     :duane {:name "Duane Allman" :born 1946 :died 1971}
     :steve {:name "Steve Vai"    :born 1960}}
    guitarist-id))

(defn calculate-age [born died current-year]
  (if died
    (- died born)
    (- current-year born)))

(defn get-current-year []
  2019)

;;
;; The steps
;;

(def get-guitarist-step
  {:pipeline.step/name :get-guitarist
   :pipeline.step/type :action
   :pipeline.step/function {:jimi  {:name "Jimi Hendrix" :born 1942 :died 1970}
                            :duane {:name "Duane Allman" :born 1946 :died 1971}
                            :steve {:name "Steve Vai"    :born 1960}}
   :pipeline.step/input-paths [[:guitarist-id]]
   :pipeline.step/output-path :guitarist
   :pipeline.step/output-schema :guitarist/guitarist})

(def get-guitarist-step
  (pipeline/action :get-guitarist #'get-guitarist
                   [[:guitarist-id]] :guitarist :guitarist/guitarist))

(def get-current-year-step
   (pipeline/action :get-current-year #'get-current-year [] :current-year int?))

(def calculate-age-step
   (pipeline/transformation :calculate-age #'calculate-age
                            [[:guitarist :born] [:guitarist :died] [:current-year]]
                            :guitarist/age int?))

;;
;; The pipeline
;;

(def pipeline
  (pipeline/pipeline [get-guitarist-step
                      get-current-year-step
                      calculate-age-step]))

;;
;; Example of a successful pipeline
;;

(def pipe-res (pipeline/run-pipeline pipeline {:guitarist-id :steve}))
(pipeline/success? pipe-res)
(pipeline/get-output pipe-res)
(pipeline.print/print-result pipe-res)

;;
;; Example of a pipeline that fails due to invalid output from a step
;;

(def pipe-res (pipeline/run-pipeline pipeline {:guitarist-id -1}))
(pipeline/success? pipe-res)
(pipeline.print/print-result pipe-res)
(pipeline/get-error pipe-res)

;; Example of an exception throwing pipeline
(def pipe-res (with-redefs [get-current-year #(throw (ex-info "Problem!" {:some :problem}))]
                 (pipeline/run-pipeline pipeline {:guitarist-id :steve})))
(pipeline/success? pipe-res)
(pipeline/failure? pipe-res)
(pipeline/exception? pipe-res)
(pipeline.print/print-result pipe-res)
(pipeline/get-exception pipe-res)

(comment
  (clojure.repl/pst (pipeline/get-exception pipe-res))
  (clojure.stacktrace/print-stack-trace (pipeline/get-exception pipe-res)))

