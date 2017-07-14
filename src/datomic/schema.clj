(ns datomic.schema
  (:refer-clojure :exclude [partition namespace fn])
  (:require [clojure.string :as str]
            [clojure.set :as set]))

(defrecord Entity [partition ns schemas
                   key-mappings coercions spec])

(defn entity? [ent]
  (or (instance? datomic.schema.Entity ent)
      (::entity? (meta ent))))

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

(defn create-entity [m]
  (with-meta (map->Entity
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
        (update :schemas assoc (:db/ident part) part))))

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
                (update :db/unique #(qualify-keyword "db.unique" %))

                true
                (assoc :db.install/_attribute :db.part/db))

         spec (if (var? type)
                type
                (:db/valueType a))]
     (-> ent
         (update :schemas assoc (:db/ident a) a)
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
                        (or (:db/id ent)
                            (tempid (:partition ent))))
         depends #{(:partition ent)}]
     (-> ent
         (update :schemas assoc (:db/ident x)
                 (vary-meta x assoc
                            ::enum?   true
                            ::depends depends))
         (vary-meta assoc ::enum? true)))))

(defn fn'
  ([ent fname bindings body]
   (if-not (entity? ent)
     (fn' ent fname (cons bindings body))
     (let [fname (qualify-keyword (str "fn." (:ns ent)) fname)]
       (update ent :schemas assoc fname
               (fn' fname bindings body)))))
  ([name bindings body]
   (let [body (if (= 1 (count body))
                (first body)
                (cons 'do body))]
     {:db/ident name
      :db/fn    (datomic.api/function
                 (merge (meta bindings)
                        {:lang   "clojure"
                         :params bindings
                         :code   body}))})))

(defmacro fn [name bindings & body]
  (assert peer? "fn form must working with peer lib")
  `(fn' ~name '~bindings '~(first body) '~(rest body)))

(defn raw [ent & xs]
  (update ent :schemas merge
          (zipmap (repeatedly gensym) xs)))

(defn depends [schema]
  (->> (dissoc schema :db/ident)
       (apply concat)
       (set)
       (set/union (::depends (meta schema)))))

(defn schema-txes
  ([] (->> (all-ns)
           (mapcat (comp vals ns-publics))
           (filter entity?)
           (map var-get)
           (apply schema-txes)))
  ([& ents]
   (let [schemas (->> (map :schemas ents)
                      (apply merge-with merge))]
     (loop [txes   ()
            idents (set (keys schemas))]
       (if (empty? idents)
         txes
         (let [deps  (->> (map schemas idents)
                          (map depends)
                          (apply set/union))
               tx    (->> (set/difference idents deps)
                          (map schemas))
               inter (set/intersection deps idents)]
           (recur (cons tx txes) inter)))))))

(defn camel->ns [clazz]
  (-> (name clazz)
      (str/replace-first #"\w" str/lower-case)
      (str/replace #"[A-Z]" #(str "." (str/lower-case %)))
      (symbol)))

(defn wrap-key-mappings [{:as ent schemas :schemas}]
  (let [idents (keys schemas)]
    (assoc ent :key-mappings
           (zipmap (map (comp keyword name) idents) idents))))


(declare coerce)

(defn satisfy-entity [{:as ent :keys [key-mappings coercions]} m]
  (if (enum? ent)
    (qualify-keyword (:ns ent) m)
    (reduce-kv (clojure.core/fn [e k v]
                 (let [k (key-mappings k k)
                       c (coercions k)]
                   (assoc e k (coerce c v))))
               {}
               m)))

(defn coerce [c m]
  (cond
    (var? c)
      (coerce (var-get c) m)

    (entity? c)
      (if (sequential? m)
        (map #(satisfy-entity c %) m)
        (satisfy-entity c m))

    :else m))

(defmacro defentity [name & decls]
  `(do
     (def ~(with-meta name {::entity? true})
       (-> {:partition :db.part/user
            :ns        '~(camel->ns name)
            :schemas   {}
            :coercions {}}
           (with-meta {::entity? true})
           ~@decls
           (wrap-key-mappings)
           (vary-meta assoc ::entity? true)
           (create-entity)))
     (defn ~(symbol (str "->" name)) [m#]
       (coerce ~name m#))))
