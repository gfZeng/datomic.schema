(ns datomic.schema-test
  (:require [clojure.test :refer :all]
            [datomic.schema :as schema :refer [defschema]]
            [datomic.api :as d]))


(declare UserRole Species)

(defschema User
  (schema/attrs
   [:name               :string    {:unique :identity}]
   [:email              :string    {:unique :identity}]
   [:biological/species #'Species  {:cardinality :many}]
   [:role               #'UserRole {:cardinality :many}]
   [:age                :long      {:index true}]))

(defschema ReDefine
  (schema/attrs
   [:user/name :string {:unique :identity}]))

(defschema Species
  ;; attributes for Species enumeration
  (schema/attrs
   [:belongs-to #'Species])

  (schema/enums

   ;; `:animal` enum, both working for `:animal` and `:species/animal`
   :animal

   ;; `:human` enum
   ;; `:human` belongs to `:animal` used `:species/belongs-to` attribute.
   ;; so, we must install attribute `:species/belongs-to` first.
   ;; It's ok, `schema/install` can handle this case
   {:db/ident           :human
    :species/belongs-to :species/animal}
   {:db/ident           :woman
    :species/belongs-to :species/human}
   {:db/ident   :man
    :belongs-to :species/human}

   ;; a robot
   :robot))

(defschema UserRole
  (schema/enums :vip :admin :normal))

(defn belongs-to? [s1 s2]
  (when-let [super (:species/belongs-to s1)]
    (or (identical?  super (:db/ident s2))
        (belongs-to?
         (d/entity (d/entity-db s1) super)
         s2))))

(declare conn)

(defn kind-of?
  ([ent sident]
   (kind-of? (d/db conn) ent sident))
  ([db ent sident]
   (let [species-set (:biological/species ent)
         species-set (if (coll? species-set)
                       (set species-set)
                       (hash-set species-set))]
     (or (some #(identical? sident %) species-set)
         (let [s (d/entity db sident)]
           (some (fn [s']
                   (when-let [s' (d/entity db s')]
                     (belongs-to? s' s)))
                 species-set))))))

(defschema Functions

  (schema/fn :fn.species/human?
    ^{:requires [[datomic.schema-test :as dst]]}
    [ent]
    (dst/kind-of? ent :species/human))

  (schema/fn :fn.user/add [db user]
    (when-not (d/invoke db :fn.species/human? user)
      (throw (ex-info "not an human"
                      {:biological/species (:biological/species user)}))
      [user])))

(defschema SelfDepends
  (schema/attrs
   [:foo #'SelfDepends])
  (schema/raws
   {:db/doc "hello"}
   {:db/id            :self.depends/foo
    :self.depends/foo :self.depends/foo}))


(def conn
  (let [uri "datomic:mem://test"]
    (d/create-database uri)
    (d/connect uri)))


;; `schema/install` can handle attribute dependencies
(schema/install conn *ns*)

(defschema Foo
  (schema/attrs
   [:bar :string]))

@(d/transact conn [{:db/ident              :user/email
                    :db/valueType          :db.type/string
                    :db/index              true}])

(deftest schema-api-test
  (let [t (d/basis-t (d/db conn))]
    (schema/install conn User UserRole Functions)
    (is (= (d/basis-t (d/db conn)) t)))

  (let [t (d/basis-t (d/db conn))]
    (schema/install conn User UserRole Functions Foo)
    (is (= (d/basis-t (d/db conn)) (inc t))))

  (is (= (-> ReDefine :tx-data :user/name)
         (-> User :tx-data :user/name)))
  (is @(d/transact conn [(->> {:db/id "user1"
                               :name  "isaac"
                               :age   18
                               :role  [:vip :admin]}
                              (->User))]))
  (is @(d/transact conn [(->> {:db/id      "user2"
                               :user/email "goood"
                               :user/role  :user.role/vip}
                              (->User))]))

  (is @(d/transact conn [(schema/fn :abc/foo [args]
                           (prn args))]))

  (is (= (with-out-str (d/invoke (d/db conn) :abc/foo [1 2 3]))
         "[1 2 3]\n"))

  (is @(d/transact
        conn
        [[:fn.user/add
          {:db/id              "user2"
           :user/name          "Madame Curie"
           :user/email         "goood"
           :user/role          :user.role/vip
           :biological/species :species/woman}]]))

  (is (thrown? Exception
               @(d/transact
                 conn
                 [[:fn.user/add
                   {:db/id              "user2"
                    :user/name          "Madame Curie"
                    :user/role          :user.role/vip
                    :biological/species [:species/robot]}]]))))

(when (try
        (require '[clojure.spec.alpha :as s])
        true
        (catch Exception e))
  (load "spec-impl-test"))
