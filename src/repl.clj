(ns repl
  (:require 
   [clojure.pprint :refer [pprint]]
   [clojure.tools.namespace.repl]))

(defn refresh []
  (clojure.tools.namespace.repl/refresh)) 

(defn eval-buffer [filename]
  (load-file filename))
