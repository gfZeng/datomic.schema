
(in-ns 'datomic.schema-test)

(require '[clojure.spec.alpha :as s])

(deftest spec-test
  (let [user  (->> {:db/id "user1"
                    :name  "isaac"
                    :age   18
                    :role  [:vip :admin]}
                   (->User))
        user2 (->> {:name "albert"
                    :age  18
                    :role [:vip :admin]}
                   (->User))]
    (is (s/valid? :db.type/bigint 3N))
    (is (s/valid? :db.type/bigint (BigInteger. "3")))
    (is (= user (->> user
                     (s/conform User)
                     (s/unform User))))
    (is (= user2 (->> user2
                     (s/conform User)
                     (s/unform User))))))

