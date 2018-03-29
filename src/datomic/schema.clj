(ns datomic.schema
  (:refer-clojure :exclude [partition namespace fn])
  (:require [clojure.string :as str]
            [clojure.set :as set]
            [clojure.walk :as walk]))

(defrecord Schema [partition ns tx-data
                   key-mappings coercions spec])

(defn schema? [ent]
  (or (instance? datomic.schema.Schema ent)
      (::schema? (meta ent))))

(defn enum? [ent]
  (::enum? (meta ent)))

(defn partition? [ent]
  (or (:db.install/_partition ent)
      (::partition? (meta ent))))

(when (try
        (require '[clojure.spec.alpha :as s])
        true
        (catch Exception e))
  (load "spec-impl"))

(defn create-schema [m]
  (with-meta (map->Schema
              (assoc m :spec (atom nil)))
    (meta m)))

(def peer?
  (try
    (require 'datomic.api)
    true
    (catch Throwable e
      false)))

(if peer?
  (def tempid @(resolve 'datomic.api/tempid))
  (defn tempid
    ([part]    (.toString (java.util.UUID/randomUUID)))
    ([part id] (str id))))

(defn qualify-keyword [ns k]
  (let [k (keyword k)]
    (if (or (nil? ns) (clojure.core/namespace k))
      k
      (keyword (name ns) (name k)))))

(defn map-keys [f m]
  (reduce-kv #(assoc %1 (f %2) %3) {} m))

(defn partition [ent part]
  (assert peer? "partition form must working with peer lib")
  (let [part (if (map? part)
               part
               {:ident part})
        part (map-keys #(qualify-keyword "db" %) part)
        part (assoc part :db.install/_partition :db.part/db)]
    (-> ent
        (assoc  :partition (:db/ident part))
        (update :tx-data assoc (:db/ident part) part))))

(defn namespace [ent ns]
  (assoc ent :ns ns))

(defn attrs
  ([ent a & as]
   (reduce attrs ent (cons a as)))
  ([ent a]
   (let [a    (if (map? a)
                a
                (let [[ident type opts] a]
                  (merge opts
                         {:ident     ident
                          :valueType type})))
         a    (map-keys #(qualify-keyword "db" %) a)
         type (:db/valueType a)
         a    (cond-> a
                true
                (update :db/ident #(qualify-keyword (:ns ent) %))

                true
                (update :db/valueType
                        #(if (var? %)
                           :db.type/ref
                           (qualify-keyword "db.type" %)))

                true
                (update :db/cardinality
                        #(if %
                           (qualify-keyword "db.cardinality" %)
                           :db.cardinality/one))

                (:db/unique a)
                (update :db/unique #(qualify-keyword "db.unique" %)))

         spec (if (var? type)
                type
                (:db/valueType a))]
     (-> ent
         (update :tx-data assoc (:db/ident a) a)
         (assoc-in [:coercions (:db/ident a)] spec)))))

(defn enums
  ([ent x & xs]
   (reduce enums ent (cons x xs)))
  ([ent x]
   (let [x       (if (map? x)
                   x
                   {:db/ident x})
         ns      (:ns ent)
         x       (update x :db/ident #(qualify-keyword ns %))
         x       (map-keys #(qualify-keyword ns %) x)
         x       (assoc x :db/id
                        (or (:db/id x)
                            (tempid (:partition ent))))
         depends #{(:partition ent)}]
     (-> ent
         (update :tx-data assoc (:db/ident x)
                 (vary-meta x assoc
                            ::enum?   true
                            ::depends depends))
         (vary-meta assoc ::enum? true)))))

(defn fn'
  ([ent fname bindings body]
   (if-not (schema? ent)
     (fn' ent fname (cons bindings body))
     (let [fname (qualify-keyword (str "fn." (:ns ent)) fname)]
       (update ent :tx-data assoc fname
               (fn' fname bindings body)))))
  ([name bindings body]
   (let [body (if (= 1 (count body))
                (first body)
                (cons 'do body))]
     {:db/ident name
      :db/fn    ((resolve 'datomic.api/function)
                 (merge (meta bindings)
                        {:lang   "clojure"
                         :params bindings
                         :code   body}))})))

(when-not peer?
  (defn fn' [& args]
    (println "Peer lib required")))

(defmacro fn [name bindings & body]
  (assert peer? "fn form must working with peer lib")
  `(fn' ~name '~bindings '~(first body) '~(rest body)))

(defn raws [ent & xs]
  (update ent :tx-data merge
          (zipmap (repeatedly gensym) xs)))

(defn depends [schema]
  (->> (dissoc schema :db/ident)
       (apply concat)
       (set)
       (set/union (::depends (meta schema)))))

(defn schemas
  ([] (apply schemas (all-ns)))
  ([& nses]
   (->> nses
        (mapcat (comp vals ns-publics))
        (filter schema?)
        (map var-get))))

(defn tx-datas
  ([] (apply tx-datas (schemas)))
  ([& ents]
   (let [tx-data (->> (map :tx-data ents)
                      (apply merge-with merge))]
     (loop [datas  ()
            idents (set (keys tx-data))]
       (if (empty? idents)
         datas
         (let [deps  (->> (map tx-data idents)
                          (map depends)
                          (apply set/union))
               data  (->> (set/difference idents deps)
                          (map tx-data))
               inter (set/intersection deps idents)]
           (recur (cons data datas) inter)))))))

(defn camel->ns [clazz]
  (-> (name clazz)
      (str/replace-first #"\w" str/lower-case)
      (str/replace #"[A-Z]" #(str "." (str/lower-case %)))
      (symbol)))

(defn wrap-key-mappings [{:as ent tx-data :tx-data}]
  (let [idents (keys tx-data)]
    (assoc ent :key-mappings
           (zipmap (map (comp keyword name) idents) idents))))


(declare coerce)

(defn satisfy-schema [{:as ent :keys [partition key-mappings coercions]} m]
  (if (enum? ent)
    (qualify-keyword (:ns ent) m)
    (reduce-kv (clojure.core/fn [e k v]
                 (let [k (key-mappings k k)
                       c (coercions k)]
                   (assoc e k (coerce c v))))
               {:db/id (or (:db/id m) (tempid partition))}
               m)))

(defn coerce [c m]
  (cond
    (var? c)
      (coerce (var-get c) m)

    (schema? c)
      (if (sequential? m)
        (map #(satisfy-schema c %) m)
        (satisfy-schema c m))

    :else m))

(defmacro defschema [name & decls]
  `(do
     (def ~(with-meta name {::schema? true})
       (-> {:partition :db.part/user
            :ns        '~(camel->ns name)
            :tx-data   {}
            :coercions {}}
           (with-meta {::schema? true})
           ~@decls
           (wrap-key-mappings)
           (vary-meta assoc ::schema? true)
           (create-schema)))
     (defn ~(symbol (str "->" name)) [m#]
       (coerce ~name m#))))

(defmacro defentity
  {:deprecated "0.1.7"}
  [name & decls]
  `(defschema ~name ~@decls))

(defn- peer-conn? [conn]
  (not (:db-id conn)))


(defmacro with-alias [aliases & body]
  (let [names    (map str aliases)
        ns-names (set names)
        aliases  (apply array-map names)
        w        (clojure.core/fn w [body]
                   (walk/walk
                    (clojure.core/fn [x]
                      (cond
                        (and (symbol? x)
                             (ns-names (clojure.core/namespace x)))
                          (let [ns  (clojure.core/namespace x)
                                ns  (aliases ns ns)
                                sym (symbol ns (name x))]
                            `(var-get (resolve '~sym)))

                        (coll? x)
                          (w x)

                        :else
                          x))
                    identity
                    body))]
    (cons
     'do
     (w body))))

(defn install
  ([conn] (apply install conn (schemas)))
  ([conn & schemas-or-nses]
   (with-alias [d datomic.api
                c datomic.client.api]
     (let [scms  (mapcat #(if (schema? %)
                            [%]
                            (schemas (the-ns %)))
                         schemas-or-nses)
           peer? (peer-conn? conn)
           trans (if peer?
                   #(deref (d/transact conn %))
                   #(c/transact conn {:tx-data %}))

           updated? (clojure.core/fn [tx-data]
                      (let [report (if peer?
                                     (d/with (d/db conn) tx-data)
                                     (c/with (c/with-db conn) {:tx-data tx-data}))]
                        (> (count (:tx-data report)) 1)))]
       (doseq [tx    (apply tx-datas scms)
               :when (updated? tx)]
         (trans tx))))))
