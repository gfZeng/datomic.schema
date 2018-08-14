
(in-ns 'datomic.schema)
(require '[clojure.spec.alpha :as s])


(s/def :db.type/keyword keyword?)
(s/def :db.type/string  string?)
(s/def :db.type/boolean boolean?)
(s/def :db.type/long    int?)
(s/def :db.type/bigint  integer?)
(s/def :db.type/float   float?)
(s/def :db.type/double  double?)
(s/def :db.type/bigdec  decimal?)
(s/def :db.type/instant inst?)
(s/def :db.type/uuid    uuid?)
(s/def :db.type/bytes   bytes?)
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
                            :bigdec  :db.type/bigdec
                            :instant :db.type/instant))

(defn- schema-spec [{:as ent :keys [coercions tx-data]}]
  (if (enum? ent)
    (eval `(s/spec ~(into #{}
                          (comp (filter enum?)
                             (map :db/ident))
                          (vals tx-data))))
    (do
      (doseq [[k spec] coercions
              :let     [spec  (s/specize* spec)
                        many? (-> tx-data
                                  (get k)
                                  (:db/cardinality)
                                  #{:db.cardinality/many})]]
        (eval `(s/def ~k ~(if many?
                            (s/or :one  spec
                                  :many (s/+ spec))
                            spec))))
      (eval `(s/keys :opt ~(keys coercions))))))


(extend-protocol s/Specize
  datomic.schema.Schema
  (specize*
    ([this]
     (let [spec (:spec this)]
       (or @spec
           (swap! spec #(or % (schema-spec this))))))
    ([this _]
     (s/specize* this)))

  clojure.lang.Var
  (specize*
    ([v]   (s/specize* (var-get v)))
    ([v _] (s/specize* (var-get v)))))
