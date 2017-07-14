
(in-ns 'datomic.schema-test)

(require '[clojure.spec.alpha :as s])

(deftest spec-test
  (let [user (->> {:db/id "user1"
                   :name  "isaac"
                   :age   18
                   :role  [:vip :admin]}
                  (->User))]
    (is (= user (->> user
                     (s/conform User)
                     (s/unform User))))))

