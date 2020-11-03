(ns pipeline.core-test
  (:require
    [clojure.test :refer :all]
    ;[clojure.spec.alpha :as s]
    [clojure.string :as str]
    [pipeline.core :as pipeline]
    [pipeline.print :refer :all]))

(defn db-execute!
  "A thin wrapper around jdbc/execute! that is callable with sql statement separated from the args"
  [data-source sql-statement & args]
  (case [sql-statement args]
    ["select * from balance where user_id = ?" [2]]
    [{:balance/balance 10000.0
      :balance/currency "SEK"
      :balance/id 2
      :balance/user_id 2}
     {:balance/balance 1000.0
      :balance/currency "USD"
      :balance/id 3
      :balance/user_id 2}]))

(defn get-exchange-rates! [base-url date base symbols]
  (let [options  {:query-params {"base" base "symbols" (str/join "," symbols)}
                  :as :json}
        url (format "%s/%s" base-url date)])
    ;(http/get url options)))
  {:body {:base "EUR"
          :date "2020-06-01"
          :rates {:SEK 10.4635 :USD 1.1116}}})

(defn extract-currencies [balances]
  (map :balance/currency balances))

(defn calculate-single-value [exchange-rate-map balance-entry]
  (let [balance (:balance/balance balance-entry)
        currency (keyword (:balance/currency balance-entry))]
    (/ balance (get exchange-rate-map currency))))

(defn calculate-value [balances exchange-rates]
  (->>
    balances
    (map (partial calculate-single-value exchange-rates))
    (reduce +)))


(def step-get-balances-for-user  ;; with simple keywords on input paths
  (pipeline/action :get-balances-for-user #'db-execute! [:data-source :sql-query :user-id] :balances))

(def step-extract-currencies ;; with path as output path
  (pipeline/transformation :extract-currencies #'extract-currencies [[:balances]] [:currencies :value]))

(def step-get-exchange-rates ;; with all paths as inputs paths
  (pipeline/action :get-exchange-rates #'get-exchange-rates!
                   [[:get-exchange-rate-url] [:date-today] [:base-currency] [:currencies :value]]
                   :exchange-rates-response))

(def step-calculate-value ;; with mixed keywords and paths as input paths
  (pipeline/transformation :calculate-value #'calculate-value
                           [:balances [:exchange-rates-response :body :rates]]
                           :value double?))

(def steps
  [step-get-balances-for-user
   step-extract-currencies
   step-get-exchange-rates
   step-calculate-value])

(def example-pipeline
  (pipeline/make-pipeline
    {}
    step-get-balances-for-user
    step-extract-currencies
    step-get-exchange-rates
    step-calculate-value))

(pipeline/steps example-pipeline)

(comment
  (def ex *e)
  (s/valid? :pipeline/step (:item (ex-data ex)))
  (s/explain :pipeline/step (:item (ex-data ex)))
  (s/explain :pipeline.step/seq-id 1)
  (s/explain :pipeline.step/seq-id "1")

  (s/explain :pipeline/step {})
  (s/form :pipeline/step))

(def args
  {:date-today "2020-06-01"
   :data-source "fake data source"
   :sql-query "select * from balance where user_id = ?"
   :user-id 2
   :get-exchange-rate-url "https://api.exchangeratesapi.io"
   :base-currency "EUR"})

(deftest pipeline?-test
  (is (true? (pipeline/pipeline? example-pipeline)))
  (is (false? (pipeline/pipeline? (-> example-pipeline pipeline/steps first)))))

(deftest step?-test
  (is (false? (pipeline/step? example-pipeline)))
  (is (true? (pipeline/step? (-> example-pipeline pipeline/steps first)))))

(deftest not-started-pipeline
  (let [run example-pipeline]
    (is (= :not-started (pipeline/state run)))
    (is (true? (pipeline/not-started? run)))
    (is (false? (pipeline/successful? run)))
    (is (false? (pipeline/failed? run)))

    (is (true? (every? pipeline/not-started? (pipeline/steps run))))))

(deftest successful-pipeline
  (let [run (pipeline/run-pipeline example-pipeline args)]
    (is (= :successful (pipeline/state run)))
    (is (false? (pipeline/not-started? run)))
    (is (true? (pipeline/successful? run)))
    (is (false? (pipeline/failed? run)))
    (is (empty? (pipeline/failed-steps run)))
    (is (= 1855.3073327623074 (pipeline/result run)))))

(deftest failed-pipeline-exception
  (with-redefs [db-execute! (fn [& args] (throw (ex-info "Problem!" {:some :problem})))]
    (let [run (pipeline/run-pipeline example-pipeline {})]
      (is (= :failed (pipeline/state run)))
      (is (false? (pipeline/not-started? run)))
      (is (false? (pipeline/successful? run)))
      (is (true? (pipeline/failed? run)))
      (is (= [:get-balances-for-user]
             (map pipeline/step-name (pipeline/failed-steps run))))

      (is (= :exception (-> run pipeline/failed-step pipeline/failure-reason)))
      (is (= "Problem!" (-> run pipeline/failed-step pipeline/failure-message)))

      (let [e (-> run pipeline/failed-step pipeline/failure-value)]
        (is (= "Problem!" (.getMessage e)))
        (is (= {:some :problem} (ex-data e)))))))

(deftest failed-pipeline-validation-error
  (with-redefs [calculate-value (fn [& args] "oopsie")]
    (let [run (pipeline/run-pipeline example-pipeline args)]
      (print-run)
      (is (= :failed (pipeline/state run)))
      (is (false? (pipeline/not-started? run)))
      (is (false? (pipeline/successful? run)))
      (is (true? (pipeline/failed? run)))
      (is (= [:calculate-value]
             (map pipeline/step-name (pipeline/failed-steps run))))

      (is (= :invalid-output (-> run pipeline/failed-step pipeline/failure-reason))))))
      ;(is (= "oopsie" (-> run pipeline/failed-step pipeline/failure-message
      ;                    (get-in [:clojure.spec.alpha/problems 0 :val]))))))

(deftest pipeline-bindings-test
  (with-redefs [get-exchange-rates! (fn [base-url date base symbols]
                                      (if-not (= base-url "http://foo.com/bar")
                                        (throw (ex-info "Pipeline bindings not working!" {:base-url base-url}))
                                        {:body {:base "EUR"
                                                :date "2020-06-01"
                                                :rates {:SEK 10.4635 :USD 1.1116}}}))]

    (let [pl (pipeline/make-pipeline {:get-exchange-rate-url "http://foo.com/bar"} steps)]
      (let [run (pipeline/run-pipeline pl (dissoc args :get-exchange-rate-url))]
        (is (= :successful (pipeline/state run)))))))

(deftest step-bindings-test
  (with-redefs [get-exchange-rates! (fn [base-url date base symbols]
                                      (if-not (= base-url "http://foo.com/step")
                                        (throw (ex-info "Pipeline bindings not working!" {:base-url base-url}))
                                        {:body {:base "EUR"
                                                :date "2020-06-01"
                                                :rates {:SEK 10.4635 :USD 1.1116}}}))]

    (let [step-get-exchange-rates-with-binding
          (pipeline/action :get-exchange-rates #'get-exchange-rates!
                           [[:get-exchange-rate-url] [:date-today] [:base-currency] [:currencies :value]]
                           :exchange-rates-response
                           nil
                           {:get-exchange-rate-url "http://foo.com/step"})

          steps [step-get-balances-for-user
                 step-extract-currencies
                 step-get-exchange-rates-with-binding
                 step-calculate-value]

          pl (pipeline/make-pipeline {} steps)]
      (let [run (pipeline/run-pipeline pl (dissoc args :get-exchange-rate-url))]
        (is (= :successful (pipeline/state run)))))))

(deftest make-pipeline-test
  (let [p1 ; a pipeline made by passing variable number of steps
        (pipeline/make-pipeline
          {}
          step-get-balances-for-user
          step-extract-currencies
          step-get-exchange-rates
          step-calculate-value)

        p2 ; a pipeline made by passing a list of steps
        (pipeline/make-pipeline
          {}
          [step-get-balances-for-user
           step-extract-currencies
           step-get-exchange-rates
           step-calculate-value])

        p3 ; a pipeline made by passing variable num of pipelines
        (pipeline/make-pipeline
          {}
          (pipeline/make-pipeline
            {}
            step-get-balances-for-user
            step-extract-currencies)
          (pipeline/make-pipeline
            {}
            step-get-exchange-rates
            step-calculate-value))

        p4 ; a pipeline made by passing all kinds of combinatins
        (pipeline/make-pipeline
          {}
          (pipeline/make-pipeline
            {}
            step-get-balances-for-user
            step-extract-currencies)
          [step-get-exchange-rates]
          step-calculate-value)]

    (is (= p1 p1))
    (is (= p1 p2))
    (is (= p1 p3))
    (is (= p1 p4))
    (is (= p2 p3))))




