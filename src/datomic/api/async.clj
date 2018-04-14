(ns datomic.api.async
  (:require [clojure.core.async :as a
             :refer [go go-loop <! >! chan timeout <!!]]
            [clojure.core.async.impl.protocols :as impl]
            [datomic.api :as d]))

(defn- ex-data* [e]
  (or (ex-data e)
      (ex-data (.getCause e))))

(defn transact! [conn tx-data]
  (let [p   (a/promise-chan)
        exh (fn [e]
              (a/put! p (assoc (ex-data* e)
                               :db.error/conn conn
                               :db.error/e e
                               :db.error/tx-data tx-data)))]
    (try
      (let [fut (d/transact-async conn tx-data)]
        (d/add-listener
         fut #(try
                (a/put! p @fut)
                (catch Exception e
                  (exh e)))
         clojure.lang.Agent/pooledExecutor))
      (catch Exception e
        (exh e)))
    p))

(defn retry-when [async-pred]
  (fn [{:as report :db.error/keys [conn tx-data]}]
    (go-loop [report (assoc report :db.error/ntry 0)]
      (when (<! (async-pred report))
        (when (:db.error/e (<! (transact! conn tx-data)))
          (recur (update report :db.error/ntry inc)))))))

(def retry-always
  (let [p (a/promise-chan)]
    (a/put! p true)
    (retry-when (constantly p))))

(defn splited-pipe
  ([from splits]
   (splited-pipe from splits true))
  ([from splits close?]
   (go-loop [fly nil]
     (when-some [ch (<! splits)]
       (when fly (>! ch fly))
       (recur
        (loop []
          (if-some [x (<! from)]
            (if (>! ch x)
              (recur)
              x)
            (when close?
              (a/close! splits)
              (a/close! ch)))))))
   splits))

(defn transact-pool
  ([n]
   (transact-pool n #(go (.printStackTrace ^Exception (:db.error/e %)))))
  ([n exh]
   (let [txes   (chan)
         splits (splited-pipe txes (chan))]

     (go-loop []
       (let [ch   (chan)
             pipe (chan n)]
         (>! splits ch)
         (go-loop []
           (if-some [{:db.error/keys [conn tx-data]} (<! ch)]
             (do
               (>! pipe (transact! conn tx-data))
               (recur))
             (a/close! pipe)))

         (loop []
           (when-some [p (<! pipe)]
             (let [report (<! p)]
               (when (:db.error/e report)
                 (a/close! ch)
                 (<! (exh report))))
             (recur))))

       (when-not (impl/closed? txes)
         (recur)))

     txes)))

(defmacro flush<!
  ([ch]
   `(flush<! Long/MAX_VALUE ~ch))
  ([n ch]
   `(let [x#   (<! ~ch)
          ret# (when x# (transient [x#]))]
      (when ret#
        (loop [i# ~n]
          (if-some [x# (when (pos? i#) (a/poll! ~ch))]
            (do
              (conj! ret# x#)
              (recur (dec i#)))
            (persistent! ret#)))))))

(defn pooled
  ([conn pool]
   (pooled conn pool (chan 10)))
  ([conn pool ch]
   (go-loop []
     (if-some [tx-data (<! ch)]
       (when (>! pool {:db.error/conn    conn
                       :db.error/tx-data tx-data})
         (recur))
       (a/close! pool)))
   ch))

(defn pool-admix
  ([pool conn]
   (pool-admix pool conn (chan)))
  ([pool conn ch]
   (pool-admix pool conn ch true))
  ([pool conn ch close?]
   (go-loop []
     (when-some [tx-data (<! ch)]
       (if (>! pool {:db.error/conn    conn
                     :db.error/tx-data tx-data})
         (recur)
         (when close? (a/close! ch)))))
   ch))
