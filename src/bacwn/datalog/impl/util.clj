;;  Copyright (c) Jeffrey Straszheim. All rights reserved.  The use and
;;  distribution terms for this software are covered by the Eclipse Public
;;  License 1.0 (http://opensource.org/licenses/eclipse-1.0.php) which can
;;  be found in the file epl-v10.html at the root of this distribution.  By
;;  using this software in any fashion, you are agreeing to be bound by the
;;  terms of this license.  You must not remove this notice, or any other,
;;  from this software.
;;
;;  util.clj
;;
;;  A Clojure implementation of Datalog -- Utilities
;;
;;  straszheimjeffrey (gmail)
;;  Created 3 Feburary 2009


(ns bacwn.datalog.impl.util
  (:import (clojure.lang Reflector)))

(defn indexed
  "Returns a lazy sequence of [index, item] pairs, where items come
  from 's' and indexes count up from zero.

  (indexed '(a b c d))  =>  ([0 a] [1 b] [2 c] [3 d])"
  [s]
  (map vector (iterate inc 0) s))

(defn separate
  "Returns a vector:
   [ (filter f s), (filter (complement f) s) ]"
  [f s]
    [(filter f s) (filter (complement f) s)])

(defn subset?
  "Is set1 a subset of set2?"
  [set1 set2]
  (and (<= (count set1) (count set2))
       (every? set2 set1)))

;;; Bindings and logic vars.  A binding in a hash of logic vars to
;;; bound values.  Logic vars are any symbol prefixed with a \?.

(defn is-var?
  "Is this a logic variable: e.g. a symbol prefixed with a ?"
  [sym]
  (when (symbol? sym)
    (let [name (name sym)]
      (and (= \? (first name))
           (not= \? (fnext name))))))

(defn is-query-var?
  "Is this a query variable: e.g. a symbol prefixed with ??"
  [sym]
  (when (symbol? sym)
    (let [name (name sym)]
      (and (= \? (first name))
           (= \? (fnext name))))))

(defn map-values
  "Like map, but works over the values of a hash map"
  [f hash]
  (let [key-vals (map (fn [[key val]] [key (f val)]) hash)]
    (if (seq key-vals)
      (apply conj (empty hash) key-vals)
      hash)))

(defn keys-to-vals
  "Given a map and a collection of keys, return the collection of vals"
  [m ks]
  (vals (select-keys m ks)))

(defn reverse-map
  "Reverse the keys/values of a map"
  [m]
  (into {} (map (fn [[k v]] [v k]) m)))


;;; Preduce -- A parallel reduce over hashes
  
(defn preduce
  "Similar to merge-with, but the contents of each key are merged in
   parallel using f.

   f - a function of 2 arguments.
   data - a collection of hashes."
  [f data]
  (let [data-1 (map (fn [h] (map-values #(list %) h)) data)
        merged (doall (apply merge-with concat data-1))
        ; Groups w/ multiple elements are identified for parallel processing
        [complex simple] (separate (fn [[key vals]] (> (count vals) 1)) merged)
        fold-group (fn [[key vals]] {key (reduce f vals)})
        fix-single (fn [[key [val]]] [key val])]
    (apply merge (concat (pmap fold-group merged) (map fix-single simple)))))
  

;;; Debuging and Tracing

(def ^:dynamic *trace-datalog* nil)

(defmacro trace-datalog
  "If *test-datalog* is set to true, run the enclosed commands"
  [& body]
  `(when *trace-datalog*
     ~@body))


(declare throwable)

(defn throwf
  "Throws an Exception or Error with an optional message formatted using
  clojure.core/format. All arguments are optional:

      class? cause? format? format-args*

  - class defaults to Exception, if present it must name a kind of
    Throwable
  - cause defaults to nil, if present it must be a Throwable
  - format is a format string for clojure.core/format
  - format-args are objects that correspond to format specifiers in
    format."
  [& args]
  (throw (throwable args)))

(defn throw-if
  "Throws an Exception or Error if test is true. args are those documented
  for throwf."
  [test & args]
  (when test
    (throw (throwable args))))

(defn throw-if-not
  "Throws an Exception or Error if test is false. args are those documented
  for throwf."
  [test & args]
  (when-not test
    (throw (throwable args))))

(defn throw-arg
  "Throws an IllegalArgumentException. All arguments are optional:

        cause? format? format-args*

  - cause defaults to nil, if present it must be a Throwable
  - format is a format string for clojure.core/format
  - format-args are objects that correspond to format specifiers in
    format."
  [& args]
  (throw (throwable (cons IllegalArgumentException args))))

(defn- throwable?
  "Returns true if x is a Throwable"
  [x]
  (instance? Throwable x))

(defn- throwable
  "Constructs a Throwable with optional cause and formatted message. Its
  stack trace will begin with our caller's caller. Args are as described
  for throwf except throwable accepts them as list rather than inline."
  [args]
  (let [[arg] args
        [class & args] (if (class? arg) args (cons Exception args))
        [arg] args
        [cause & args] (if (throwable? arg) args (cons nil args))
        message (when args (apply format args))
        ctor-args (into-array Object
                              (cond (and message cause) [message cause]
                                    message [message]
                                    cause [cause]))
        throwable (Reflector/invokeConstructor class ctor-args)
        our-prefix "clojure.contrib.except$throwable"
        not-us? #(not (.startsWith (.getClassName %) our-prefix))
        raw-trace (.getStackTrace throwable)
        edited-trace (into-array StackTraceElement
                                 (drop 3 (drop-while not-us? raw-trace)))]
    (.setStackTrace throwable edited-trace)
        throwable))

;; End of file
