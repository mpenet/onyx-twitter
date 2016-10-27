(defproject org.onyxplatform/onyx-twitter "0.9.11.1"
  :description "Onyx plugin for Twitter"
  :url "https://github.com/onyx-platform/onyx-twitter"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories {"snapshots" {:url "https://clojars.org/repo"
                              :username :env
                              :password :env
                              :sign-releases false}
                 "releases" {:url "https://clojars.org/repo"
                             :username :env
                             :password :env
                             :sign-releases false}}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.onyxplatform/onyx "0.9.11"]
                 [org.twitter4j/twitter4j-core "4.0.4"]
                 [org.twitter4j/twitter4j-stream "4.0.4"]
                 [org.clojure/core.async "0.2.374"]
                 [aero "1.0.0-beta2"]
                 [org.clojure/java.data "0.1.1"]
                 [cheshire "5.6.3"] ]
  :profiles {:dev {:dependencies []
                   :plugins [[lein-set-version "0.4.1"]
                             [lein-update-dependency "0.1.2"]
                             [lein-pprint "1.1.1"]]
                   :resource-paths ["test-resources/"]}
             :circle-ci {:jvm-opts ["-Xmx4g"]}})
