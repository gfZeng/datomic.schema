(ns datomic.schema
  (:refer-clojure :exclude [partition namespace fn])
  (:require [clojure.string :as str]
            [clojure.spec :as s]))

(s/def :db.type/keyword keyword?)
(s/def :db.type/string  string?)
(s/def :db.type/boolean boolean?)
(s/def :db.type/long    int?)
(s/def :db.type/bigint  #(instance? java.math.BigInteger %))
(s/def :db.type/float   float?)
(s/def :db.type/double  double?)
(s/def :db.type/bigdec  bigdec?)
(s/def :db.type/lookup  (s/cat :key keyword?
                               :val :db/valueType))
(s/def :db.type/ident (s/or :ident  keyword?
                            :id     int?
                            :lookup :db.type/lookup))
(s/def :db/valueType  (s/or :keyword :db.type/keyword
                            :string  :db.type/string
                            :boolean :db.type/boolean
                            :long    :db.type/boolean
                            :bigint  :db.type/bigint
                            :float   :db.type/float
                            :double  :db.type/double
                            :bigdec  :db.type/bigdec))

(extend-type clojure.lang.Var
  s/Specize
  (specize*
    ([v]   (s/specize* (var-get v)))
    ([v _] (s/specize* (var-get v)))))

(declare entity-spec)
(defrecord Entity [partition ns schemas key-mappings coercions spec]
  s/Specize
  (specize* [this]
    (or @spec
        (swap! spec #(or % (entity-spec this)))))
  (specize* [this _]
    (s/specize* this)))

(defn entity? [ent]
  (or (instance? datomic.schema.Entity ent)
      (::entity? (meta ent))))

(defn enum? [ent]
  (::enum? (meta ent)))

(defn partition? [ent]
  (or (:db.install/_partition ent)
      (::partition? (meta ent))))

(defn- entity-spec [{:as ent :keys [coercions schemas]}]
  (if (enum? ent)
    (eval `(s/spec ~(into #{}
                          (comp (filter enum?)
                             (map :db/ident))
                          (vals schemas))))
    (do
      (doseq [[k spec] coercions
              :let     [spec  (s/specize* spec)
                        many? (-> schemas
                                  (get k)
                                  (:db/cardinality)
                                  #{:db.cardinality/many})]]
        (eval `(s/def ~k ~(if many?
                            (s/or :one  spec
                                  :many (s/+ spec))
                            spec))))
      (eval `(s/keys :opt ~(keys coercions))))))

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
   (let [x  (if (map? x)
              x
              {:db/ident x})
         ns (:ns ent)
         x  (update x :db/ident #(qualify-keyword ns %))
         x  (assoc x :db/id
                   (or (:db/id ent)
                       (tempid (:partition ent))))]
     (-> ent
         (update :schemas assoc (:db/ident x)
                 (vary-meta x assoc ::enum? true))
         (vary-meta assoc ::enum? true)))))

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
     (def ~name
       (-> {:partition :db.part/user
            :ns        '~(camel->ns name)
            :schemas   {}
            :coercions {}}
           ~@decls
           (wrap-key-mappings)
           (vary-meta assoc ::entity? true)
           (create-entity)))
     (defn ~(symbol (str "->" name)) [m#]
       (coerce ~name m#))))

(defmacro fn [name bindings & body]
  (assert peer? "fn form must working with peer lib")
  (let [body (if (= 1 (count body))
               (first body)
               (cons 'do body))]
    {:db/id    (tempid :db.part/db)
     :db/ident name
     :db/fn    `(datomic.api/function
                 {:lang   "clojure"
                  :params ~bindings
                  :code   ~body})}))

(defn schemas [& ents]
  (->> (map :schemas ents)
       (apply merge-with merge)
       (vals)))
