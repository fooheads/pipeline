(ns pipeline.core-test
  (:require
    [clojure.test :refer :all]
    [clojure.spec.alpha :as s]
    [pipeline.core :as pipeline]
    [pipeline.print]))

(s/def :guitarist/name string?)
(s/def :guitarist/born integer?)
(s/def :guitarist/died integer?)

(s/def :guitarist/guitarist (s/keys :req-un [:guitarist/name :guitarist/born]
                                    :opt-un [:guitarist/died]))

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
  (pipeline/action :get-guitarist #'get-guitarist
                   [[:guitarist-id]] :guitarist :guitarist/guitarist))

(def get-current-year-step
   (pipeline/action :get-current-year #'get-current-year [] :current-year int?))

(def calculate-age-step
   (pipeline/transformation :calculate-age #'calculate-age
                            [[:guitarist :born] [:guitarist :died] [:current-year]]
                            :guitarist/age int?))
(def example-pipeline
  (pipeline/pipeline [get-guitarist-step
                      get-current-year-step
                      calculate-age-step]))

(with-redefs [get-current-year (constantly nil)]
  (pipeline/run-pipeline example-pipeline {:guitarist-id :jimi}))

(deftest step-schema
  (is (true? (s/valid? :pipeline/step get-guitarist-step))))

(deftest successful-pipeline
  (let [run-pipeline #(pipeline/run-pipeline example-pipeline {:guitarist-id %})]

    (is (true? (pipeline/success? (run-pipeline :jimi))))
    (is (= 28  (pipeline/get-output (run-pipeline :jimi))))

    (is (true? (pipeline/success? (run-pipeline :steve))))
    (is (= 59  (pipeline/get-output (run-pipeline :steve))))

    (is (= 59  (pipeline/get-output (run-pipeline :steve))))))

(deftest failing-pipeline
  ;; make get-current-year step return invalid data
  (with-redefs [get-current-year (constantly nil)]
    (let [run-pipeline #(pipeline/run-pipeline example-pipeline {:guitarist-id %})
          error (pipeline/get-error (run-pipeline :steve))]

      (is (= :get-current-year (:pipeline.step/name error)))
      (is (= nil (:pipeline.error/value error))))))

(deftest exception-throwing-pipeline
  ;; make get-current-year step throw exception
  (with-redefs [get-current-year #(throw (ex-info "Problem!" {:some :problem}))]
    (let [run-pipeline #(pipeline/run-pipeline example-pipeline {:guitarist-id %})
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


