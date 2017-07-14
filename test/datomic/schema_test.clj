(ns datomic.schema-test
  (:require [clojure.test :refer :all]
            [datomic.schema :as schema :refer [defentity]]
            [datomic.api :as d]))


(declare UserRole)

(defentity User
  (schema/attrs
   [:name  :string   {:unique :identity}]
   [:email :string   {:unique :identity}]
   [:role #'UserRole {:cardinality :many}]
   [:age   :long     {:index true}]))

(defentity UserRole
  (schema/enums :vip :admin :normal))


(def conn
  (let [uri "datomic:mem://test"]
    (d/create-database uri)
    (d/connect uri)))


(doseq [tx (schema/schema-txes User UserRole)]
  @(d/transact conn tx))


(deftest schema-api-test
  (is @(d/transact conn [(->> {:db/id "user1"
                               :name  "isaac"
                               :age   18
                               :role  [:vip :admin]}
                              (->User))]))
  (is @(d/transact conn [(->> {:db/id      "user2"
                               :user/email "goood"
                               :user/role  :user.role/vip}
                              (->User))])))
