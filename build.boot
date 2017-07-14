
(set-env!
 :resource-paths #{"resources"}
 :source-paths   #{"src"}
 :dependencies   '[[org.clojure/clojure "1.9.0-alpha17"  :scope "provided"]
                   [com.datomic/datomic-pro "0.9.5561"
                    :scope "provided"
                    :exclusions [org.clojure/clojure]]

                   [adzerk/boot-test "RELEASE" :scope "test"]
                   [adzerk/bootlaces "0.1.13"  :scope "test"]])


(require '[adzerk.boot-test :refer (test)]
         '[adzerk.bootlaces :refer :all])

(def project 'datomic.schema)
(def +version+ "0.1.3")

(bootlaces! +version+)


(task-options!
 pom {:project     project
      :version     +version+
      :description "FIXME: write description"
      :url         "http://example/FIXME"
      :scm         {:url "https://github.com/yourname/datomic.schema"}
      :license     {"Eclipse Public License"
                    "http://www.eclipse.org/legal/epl-v10.html"}})

(require '[adzerk.boot-test :refer [test]])
