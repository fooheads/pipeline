(ns pipeline.core-test
  (:require 
    [clojure.test :refer :all]
    [malli.core :as m]
    [pipeline.core :as core]))

(defn get-guitarist [guitarist-id]
  (get
    {14 {:name "Jimi Hendrix" :born 1942 :died 1970}
     15 {:name "Duane Allman" :born 1946 :died 1971}
     16 {:name "Steve Vai"    :born 1960}}
    guitarist-id))

(defn calculate-age [born died current-year]
  (if died
    (- died born)
    (- current-year born)))

(def Guitarist
  (m/schema
    [:map
     [:name string?]
     [:born int?]
     [:died {:optional true} int?]]))
    
(def get-guitarist-step
  {:pipeline.step/name :get-guitarist
   :pipeline.step/type :action
   :pipeline.step/function #'get-guitarist
   :pipeline.step/input-paths [[:guitarist-id]]
   :pipeline.step/output-path :guitarist
   :pipeline.step/output-schema Guitarist})

(def get-current-year-step
  {:pipeline.step/name :get-current-year
   :pipeline.step/type :action
   :pipeline.step/function (constantly 2019)
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

(def failing-get-current-year-step
  (assoc get-current-year-step :pipeline.step/function (constantly nil)))

(def exception-get-current-year-step
  (assoc get-current-year-step :pipeline.step/function #(throw (ex-info "Problem!" {:some :problem}))))

(deftest step-schema
  (is (true? (m/validate core/Step get-guitarist-step))))

(deftest simple-pipeline
  (let [pipeline [get-guitarist-step
                  get-current-year-step
                  calculate-age-step]
        run-pipeline #(core/run-pipeline {:guitarist-id %} pipeline)]

    (is (= nil (:pipeline/error (run-pipeline 14))))
    (is (= 28 (:guitarist/age (run-pipeline 14))))

    (is (= nil (:pipeline/error (run-pipeline 16))))
    (is (= 59 (:guitarist/age (run-pipeline 16))))

    (is (= 59 (:pipeline/last-output (run-pipeline 16))))))

(deftest failing-pipeline
  (let [pipeline [get-guitarist-step
                  failing-get-current-year-step
                  calculate-age-step]
        run-pipeline #(core/run-pipeline {:guitarist-id %} pipeline)
        error (:pipeline/error (run-pipeline 16))]

    (is (= :get-current-year (:pipeline.step/name error)))
    (is (= :current-year (:key error)))))

(deftest exception-throwing-pipeline
  (let [pipeline [get-guitarist-step
                  exception-get-current-year-step
                  calculate-age-step]
        run-pipeline #(core/run-pipeline {:guitarist-id %} pipeline)
        error (:pipeline/error (run-pipeline 16))]

    (is (= :get-current-year (:pipeline.step/name error)))
    (is (= "Problem!" (-> error :details (.getMessage))))
    (is (= {:some :problem} (-> error :details ex-data)))))

(comment
  (def pipeline
    [get-guitarist-step
     get-current-year-step
     calculate-age-step])


  ;; Example of a successful pipeline
  (core/run-pipeline {:guitarist-id 16} pipeline)
  (core/print-result)
  (core/last-result)

  ;; Example of a failing pipeline
  (core/run-pipeline {:guitarist-id -1} pipeline)
  (core/print-result)
  (core/last-result)

  ;; Example of an exception throwing pipeline
  (core/run-pipeline {:guitarist-id 16} 
                     [get-guitarist-step 
                      exception-get-current-year-step 
                      calculate-age-step])
  (-> (core/last-result) :pipeline/error :details ex-data)
  (core/print-result)
  (core/last-result))


