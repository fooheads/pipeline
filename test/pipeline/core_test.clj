(ns pipeline.core-test
  (:require
    [clojure.test :refer :all]
    [clojure.spec.alpha :as s]
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

(def example-pipeline
  (pipeline/make-pipeline

    ;; with simple keywords on input paths
    [(pipeline/action
       :get-balances-for-user #'db-execute!
       [:data-source :sql-query :user-id] :balances)

     (pipeline/transformation
       :extract-currencies #'extract-currencies [[:balances]] :currencies)

     ;; with all paths as inputs paths
     (pipeline/action
       :get-exchange-rates #'get-exchange-rates!
       [[:get-exchange-rate-url] [:date-today] [:base-currency] [:currencies]]
       :exchange-rates-response)

     ;; with mixed keywords and paths as input paths
     (pipeline/transformation
       :calculate-value #'calculate-value [[:balances] [:exchange-rates-response :body :rates]] :value)]))

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
  (let [run (pipeline/run-pipeline
              example-pipeline
              {:date-today "2020-06-01"
               :data-source "fake data source"
               :sql-query "select * from balance where user_id = ?"
               :user-id 2
               :get-exchange-rate-url "https://api.exchangeratesapi.io"
               :base-currency "EUR"})]

    (is (= :successful (pipeline/state run)))
    (is (false? (pipeline/not-started? run)))
    (is (true? (pipeline/successful? run)))
    (is (false? (pipeline/failed? run)))
    (is (empty? (pipeline/failed-steps run)))))

(deftest failed-pipeline
  (with-redefs [db-execute! #(throw (ex-info "Problem!" {:some :problem}))]
    (let [run (pipeline/run-pipeline example-pipeline {})]
      (is (= :failed (pipeline/state run)))
      (is (false? (pipeline/not-started? run)))
      (is (false? (pipeline/successful? run)))
      (is (true? (pipeline/failed? run)))
      (is (= [:get-balances-for-user]
             (map pipeline/step-name (pipeline/failed-steps run)))))))


