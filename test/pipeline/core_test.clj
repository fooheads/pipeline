(ns pipeline.core-test
  (:require 
    [clojure.test :refer :all]
    [malli.core :as m]
    [pipeline.core :as pipeline]
    [pipeline.print]))

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

(def Guitarist
  (m/schema
    [:map
     [:name string?]
     [:born int?]
     [:died {:optional true} int?]]))
    
(def get-guitarist-step
  {:pipeline.step/name :get-guitarist
   :pipeline.step/type :pipeline.step.type/action
   :pipeline.step/function #'get-guitarist
   :pipeline.step/input-paths [[:guitarist-id]]
   :pipeline.step/output-path :guitarist
   :pipeline.step/output-schema Guitarist})

(def get-current-year-step
  {:pipeline.step/name :get-current-year
   :pipeline.step/type :pipeline.step.type/action
   :pipeline.step/function #'get-current-year
   :pipeline.step/input-paths []
   :pipeline.step/output-path :current-year
   :pipeline.step/output-schema int?})

(def calculate-age-step
  {:pipeline.step/name :calculate-age
   :pipeline.step/type :pipeline.step.type/transformation
   :pipeline.step/function #'calculate-age
   :pipeline.step/input-paths [[:guitarist :born] [:guitarist :died] [:current-year]]
   :pipeline.step/output-path :guitarist/age
   :pipeline.step/output-schema int?})

(def example-pipeline
  [get-guitarist-step
   get-current-year-step
   calculate-age-step])

(m/explain pipeline/Pipeline example-pipeline)

(deftest step-schema
  (is (true? (m/validate pipeline/Step get-guitarist-step))))

(deftest successful-pipeline
  (let [run-pipeline #(pipeline/run-pipeline {:guitarist-id %} example-pipeline)]

    (is (true? (pipeline/success? (run-pipeline :jimi))))
    (is (= 28  (pipeline/get-output (run-pipeline :jimi))))

    (is (true? (pipeline/success? (run-pipeline :steve))))
    (is (= 59  (pipeline/get-output (run-pipeline :steve))))

    (is (= 59  (pipeline/get-output (run-pipeline :steve))))))

(deftest failing-pipeline
  ;; make get-current-year step return invalid data
  (with-redefs [get-current-year (constantly nil)]  
    (let [run-pipeline #(pipeline/run-pipeline {:guitarist-id %} example-pipeline)
          error (pipeline/get-error (run-pipeline :steve))]

      (is (= :get-current-year (:pipeline.step/name error)))
      (is (= nil (:pipeline.error/value error))))))

(deftest exception-throwing-pipeline
  ;; make get-current-year step throw exception
  (with-redefs [get-current-year #(throw (ex-info "Problem!" {:some :problem}))]
    (let [run-pipeline #(pipeline/run-pipeline {:guitarist-id %} example-pipeline)
          result (run-pipeline :steve)
          error (pipeline/get-error result)]

      (def *foo result)


      (is (= :get-current-year (:pipeline.step/name error)))
      (is (= "Problem!" (:pipeline.error/message error)))
      (is (= {:some :problem} (-> error :pipeline.error/value ex-data)))
      (is (not= nil (re-find #"(?im):get-current-year" 
                             (with-out-str (pipeline.print/print-result)))))
      (is (not= nil (re-find #"(?im)Problem" 
                             (with-out-str (pipeline.print/print-result))))))))


(comment
  (def pipeline
    [get-guitarist-step
     get-current-year-step
     calculate-age-step])

  ;;; TODO: Update and move to README

  ;; Example of a successful pipeline
  (pipeline/run-pipeline {:guitarist-id :steve} pipeline)
  (pipeline/print-result)
  (pipeline/last-result)

  ;; Example of a failing pipeline
  (pipeline/run-pipeline {:guitarist-id -1} pipeline)
  (pipeline/print-result)
  (pipeline/last-result)

  ;; Example of an exception throwing pipeline
  (pipeline/run-pipeline {:guitarist-id :steve} 
                     [get-guitarist-step 
                      exception-get-current-year-step 
                      calculate-age-step])
  (pipeline/print-result)
  (pipeline/last-result))



(def pipeline
  [{:jimi  {:name "Jimi Hendrix" :born 1942 :died 1970}
    :duane {:name "Duane Allman" :born 1946 :died 1971}
    :steve {:name "Steve Vai"    :born 1960}}
   get-current-year-step
   calculate-age-step])


(def get-guitarist-step
  {:pipeline.step/name :get-guitarist
   :pipeline.step/type :pipeline.step.type/action
   :pipeline.step/function   {:jimi  {:name "Jimi Hendrix" :born 1942 :died 1970}
                              :duane {:name "Duane Allman" :born 1946 :died 1971}
                              :steve {:name "Steve Vai"    :born 1960}}
   :pipeline.step/input-paths [[:guitarist-id]]
   :pipeline.step/output-path :guitarist
   :pipeline.step/output-schema Guitarist})

(def get-guitarist-step
  {:pipeline.step/name :get-guitarist
   :pipeline.step/type :pipeline.step.type/action
   :pipeline.step/function identity
   :pipeline.step/input-paths [[:guitarists]]
   :pipeline.step/output-path :all-guitarists})
   ;:pipeline.step/output-schema Guitarist})

(def get-current-year-step
  {:pipeline.step/name :get-current-year
   :pipeline.step/type :pipeline.step.type/action
   :pipeline.step/function #'get-current-year
   :pipeline.step/input-paths []
   :pipeline.step/output-path :current-year
   :pipeline.step/output-schema int?})

(def calculate-age-step
  {:pipeline.step/name :calculate-age
   :pipeline.step/type :pipeline.step.type/transformation
   :pipeline.step/function #'calculate-age
   :pipeline.step/input-paths [[:guitarist :born] [:guitarist :died] [:current-year]]
   :pipeline.step/output-path :guitarist/age
   :pipeline.step/output-schema int?})

(def pipeline
  [get-guitarist-step])
   ;get-current-year-step
   ;calculate-age-step])

(def data
  {:guitarists
    {:jimi  {:name "Jimi Hendrix" :born 1942 :died 1970}
     :duane {:name "Duane Allman" :born 1946 :died 1971}
     :steve {:name "Steve Vai"    :born 1960}}}) 

(pipeline/run-pipeline data pipeline)

(pipeline/run-pipeline {:guitarist-id :steve} pipeline)
(require '[malli.core :as m])
(m/explain pipeline/Pipeline pipeline)


#_({:jimi  {:name "Jimi Hendrix" :born 1942 :died 1970
                                 :duane {:name "Duane Allman" :born 1946 :died 1971}
                                 :steve {:name "Steve Vai"    :born 1960}}})
