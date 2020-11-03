(ns pipeline.default-validations
  (:require
    [clojure.spec.alpha :as s]))

(defn- valid? [context spec v]
  (s/valid? spec v))

(defn- explain-data [context spec v]
  (s/explain-data spec v))

(def default-validation-fns
  {:valid? valid?
   :explain explain-data})


