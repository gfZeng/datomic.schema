(ns datomic.client-test
  (:require  [datomic.client     :as client]
             [clojure.core.async :as a :refer [<!!]]
             [datomic.schema     :as schema :refer [defschema]]
             [datomic.schema-test]

             [clojure.test :refer :all]))

(def conn
  (<!! (client/connect
        {:db-name    "hello"
         :account-id client/PRO_ACCOUNT
         :secret     "mysecret"
         :region     "none"
         :endpoint   "localhost:8998"
         :service    "peer-server"
         :access-key "myaccesskey"})))

(declare Gender)

(defschema Person
  (schema/attrs
   [:id       :string {:unique :identity}]
   [:name     :string]
   [:gender   #'Gender]
   [:birthday :instant]))

(def NS (ns-name *ns*))

(defschema Gender
  (schema/enums
   {:db/id    "male"
    :db/ident :gender/male}
   {:db/id    "female"
    :db/ident :gender/female}))

(deftest client-conn-test
  (when-not (client/error? conn)
    (schema/install conn NS)
    (let [t (-> conn :state deref :t)]
      (schema/install conn NS)
      (is (= (-> conn :state deref :t) t)))))
