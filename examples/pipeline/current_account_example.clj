(ns pipeline.current-account-example
  (:require
    [clj-http.client :as http]
    [clojure.spec.alpha :as s]
    [clojure.string :as str]
    [next.jdbc :as jdbc]
    [next.jdbc.result-set :as result-set]
    [pipeline.core :as pipeline]
    [pipeline.print :refer :all]))

;; curl "https://api.exchangeratesapi.io/2020-06-01?base=EUR&symbols=SEK,USD"
;; {"rates":{"USD":1.1136,"SEK":10.487},"base":"EUR","date":"2020-05-29"}%
;; {:status 200
;;  :body {:base "EUR" :date "2020-05-29" :rates {:SEK 10.487 :USD 1.1136}}}

;;
;; Helper functions
;;

(declare db-execute!)

(defn populate-database [ds]
  (db-execute! ds "create table user (
                      id int auto_increment primary key,
                      name varchar)")

  (db-execute! ds "create table balance (
                  id int auto_increment primary key,
                  currency varchar(3) not null,
                  balance double not null,
                  user_id int not null,
                  foreign key (user_id) references user(id))")

  (jdbc/execute! ds ["insert into user(name) values('John Doe')"])
  (jdbc/execute! ds ["insert into user(name) values('Jane Doe')"])

  (jdbc/execute! ds ["insert into balance(currency,balance,user_id) values('SEK',12380,1)"])
  (jdbc/execute! ds ["insert into balance(currency,balance,user_id) values('SEK',10000,2)"])
  (jdbc/execute! ds ["insert into balance(currency,balance,user_id) values('USD',1000,2)"]))

;;
;; These are the functions used in the pipeline
;;

(defn db-execute!
  "A thin wrapper around jdbc/execute! that is callable with sql statement separated from the args"
  [data-source sql-statement & args]
  (jdbc/execute! data-source (cons sql-statement args) {:builder-fn result-set/as-lower-maps}))

(defn get-exchange-rates! [base-url date base symbols]
  (let [options  {:query-params {"base" base "symbols" (str/join "," symbols)}
                  :as :json}
        url (format "%s/%s" base-url date)]
    (http/get url options)))

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


;;
;; Define the steps and the pipeline
;;

(def example-pipeline
  (pipeline/make-pipeline
    [(pipeline/action
       :get-balances-for-user #'db-execute!
       [[:data-source] [:sql-query] [:user-id]]

       :balances)

     (pipeline/transformation
       :extract-currencies #'extract-currencies [[:balances]] :currencies)

     (pipeline/action
       :get-exchange-rates #'get-exchange-rates!
       [[:get-exchange-rate-url] [:date-today] [:base-currency] [:currencies]]
       :exchange-rates-response)

     (pipeline/transformation
       :calculate-value #'calculate-value [[:balances] [:exchange-rates-response :body :rates]] :value)]
    {:get-exchange-rate-url "https://api.exchangeratesapi.io"}))

(pipeline/step-path example-pipeline (get-in example-pipeline [:pipeline/steps 2]))

(comment
  (def db {:dbtype "h2:mem" :dbname "example"})
  (def ds (jdbc/get-datasource db))

  (populate-database ds)

  (pipeline/run-pipeline example-pipeline {:date-today "2020-06-01"
                                           :data-source ds
                                           :sql-query "select * from balance where user_id = ?"
                                           :user-id 2
                                           ;:get-exchange-rate-url "https://api.exchangeratesapi.io"
                                           :base-currency "EUR"})

  (-> (pipeline/last-run) :pipeline/steps (nth 2) pipeline/state)

  (map :pipeline/state (-> (pipeline/last-run) :pipeline/steps))
  (map :pipeline.step/state (-> (pipeline/last-run) :pipeline/steps))
  (map pipeline/state (-> (pipeline/last-run) :pipeline/steps))
  (map pipeline/failed? (-> (pipeline/last-run) :pipeline/steps))
  (map pipeline/successful? (-> (pipeline/last-run) :pipeline/steps))

  (-> (pipeline/last-run) :pipeline/steps (nth 1) pipeline/failed?)
  (-> (pipeline/last-run) :pipeline/steps (nth 2) pipeline/failed?)

  (pipeline/state example-pipeline)
  (print-run)

  (s/valid? :pipeline/pipeline example-pipeline)
  (s/explain :pipeline/pipeline example-pipeline)

  (s/valid? :pipeline/pipeline (pipeline/last-run))
  (s/explain :pipeline/pipeline (pipeline/last-run))

  (pipeline/last-run)
  (pipeline/result)

  (pipeline/failed-step)
  (pipeline/failed-call)
  (pipeline/step-runs)
  (pipeline/step-runs')


  (print-failed-call)

  (print-pipeline)
  (print-run)

  (into [] (-> (pipeline/last-run) (pipeline/step-runs) first :pipeline.step-execution/args))
  (def step-run (-> (pipeline/last-run) (pipeline/step-runs) first))
  (augment-step-run step-run))

