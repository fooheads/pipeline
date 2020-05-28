(ns pipeline.example
  (:require
    [malli.core :as m]
    [pipeline.core :as pipeline]
    [pipeline.print]))

(def Guitarist
  (m/schema
    [:map
     [:name string?]
     [:born int?]
     [:died {:optional true} int?]]))

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

(def get-guitarist-step
  {:pipeline.step/name :get-guitarist
   :pipeline.step/type :action
   :pipeline.step/function {:jimi  {:name "Jimi Hendrix" :born 1942 :died 1970}
                            :duane {:name "Duane Allman" :born 1946 :died 1971}
                            :steve {:name "Steve Vai"    :born 1960}}
   :pipeline.step/input-paths [[:guitarist-id]]
   :pipeline.step/output-path :guitarist
   :pipeline.step/output-schema Guitarist})

(def get-guitarist-step
  {:pipeline.step/name :get-guitarist
   :pipeline.step/type :action
   :pipeline.step/function identity
   :pipeline.step/input-paths [[:guitarists]]
   :pipeline.step/output-path :all-guitarists})
 ;:pipeline.step/output-schema Guitarist})

(def get-current-year-step
  {:pipeline.step/name :get-current-year
   :pipeline.step/type :action
   :pipeline.step/function #'get-current-year
   :pipeline.step/input-paths []
   :pipeline.step/output-path :current-year
   :pipeline.step/output-schema int?})

(def calculate-age-step
  {:pipeline.step/name :calculate-age
   :pipeline.step/type :transformation
   :pipeline.step/function #'calculate-age
   :pipeline.step/input-paths [[:guitarist :born] [:guitarist :died] [:current-year]]
   :pipeline.step/output-path :guitarist/age
   :pipeline.step/output-schema int?})

(def pipeline
  [{:jimi  {:name "Jimi Hendrix" :born 1942 :died 1970}
    :duane {:name "Duane Allman" :born 1946 :died 1971}
    :steve {:name "Steve Vai"    :born 1960}}
   get-current-year-step
   calculate-age-step])


(def pipeline
  [get-guitarist-step
   get-current-year-step
   calculate-age-step])

(def data
  {:guitarists
    {:jimi  {:name "Jimi Hendrix" :born 1942 :died 1970}
     :duane {:name "Duane Allman" :born 1946 :died 1971}
     :steve {:name "Steve Vai"    :born 1960}}})

(->>
  (pipeline/run-pipeline pipeline data)
  (pipeline/get-output))

(pipeline/run-pipeline pipeline {:guitarist-id :steve})
(require '[malli.core :as m])
(m/explain pipeline/Pipeline pipeline)


;; Example of a successful pipeline
(pipeline/run-pipeline pipeline {:guitarist-id :steve})
(pipeline/success?)
(pipeline/get-output)
(pipeline.print/print-result)

(pipeline.print/print-result)
(pipeline/last-result)

;; Example of a failing pipeline
(pipeline/run-pipeline pipeline {:guitarist-id -1})
(pipeline.print/print-result)
(pipeline/last-result)

;; Example of an exception throwing pipeline
(with-redefs [get-current-year #(throw (ex-info "Problem!" {:some :problem}))]

  (pipeline/run-pipeline pipeline {:guitarist-id :steve})
  (pipeline.print/print-result)
  (pipeline/last-result))

