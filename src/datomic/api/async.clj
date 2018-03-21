(ns datomic.api.async
  (:require [clojure.core.async :as a
             :refer [go go-loop <! >! chan timeout <!!]]
            [clojure.core.async.impl.protocols :as impl]
            [datomic.api :as d]))

(defn- ex-data* [e]
  (or (ex-data e)
      (ex-data (.getCause e))))

(defn transact [conn tx-data]
  (let [p   (a/promise-chan)
        exh (fn [e]
              (a/put! p (assoc (ex-data* e)
                               ::conn conn
                               ::e e
                               ::tx-data tx-data)))]
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
  (fn [{:as report ::keys [conn tx-data]}]
    (go-loop [report (assoc report ::ntry 0)]
      (when (<! (async-pred report))
        (when (::e (<! (transact conn tx-data)))
          (recur (update report ::ntry inc)))))))

(def retry-always
  (let [p (a/promise-chan)]
    (a/put! p true)
    (retry-when-error (constantly p))))

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
              (a/close! ch)))))))))

(defn transact-pool
  ([n]
   (transact-pool n #(go (.printStackTrace ^Exception (::e %)))))
  ([n exh]
   (let [txes   (chan)
         splits (chan)]

     (splited-pipe txes splits)

     (go-loop []
       (let [ch   (chan)
             pipe (chan n)]
         (>! splits ch)
         (go-loop []
           (if-some [{::keys [conn tx-data]} (<! ch)]
             (do
               (>! pipe (transact conn tx-data))
               (recur))
             (a/close! pipe)))

         (loop []
           (when-some [p (<! pipe)]
             (let [report (<! p)]
               (when (::e report)
                 (a/close! ch)
                 (<! (exh report))))
             (recur))))

       (when-not (impl/closed? txes)
         (recur)))

     txes)))

(defn pooled
  ([conn pool]
   (pooled conn pool (chan 10)))
  ([conn pool ch]
   (go-loop []
     (if-some [tx-data (<! ch)]
       (when (>! pool {::conn    conn
                       ::tx-data tx-data})
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
       (if (>! pool {::conn    conn
                     ::tx-data tx-data})
         (recur)
         (when close? (a/close! ch)))))
   ch))
