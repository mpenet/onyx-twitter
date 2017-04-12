(ns onyx.tasks.twitter
  (:require [schema.core :as s]
            [onyx.schema :as os]))

(def TwitterStreamTaskMap
  {:twitter/consumer-key s/Str
   :twitter/consumer-secret s/Str
   :twitter/access-token s/Str
   :twitter/access-secret s/Str
   (s/optional-key :twitter/keep-keys) (s/either s/Keyword [s/Any])
   (s/optional-key :twitter/track) [s/Str]
   (s/optional-key :follow) [Long]
   (s/optional-key :filter-level) s/Str
   (s/optional-key :language) [s/Str]
   (s/optional-key :locatons) [[Double]]
   (os/restricted-ns :twitter) s/Any})

(s/defn stream
  ([task-name :- s/Keyword opts]
   {:task {:task-map (merge
                      {:onyx/name task-name
                       :onyx/plugin :onyx.plugin.twitter/consume-tweets
                       :onyx/type :input
                       :onyx/max-peers 1
                       :onyx/medium :twitter}
                      opts)
           :lifecycles [{:lifecycle/task task-name
                         :lifecycle/calls :onyx.plugin.twitter/twitter-reader-calls}]}
    :schema {:task-map TwitterStreamTaskMap}})
  ([task-name :- s/Keyword
    keep-keys :- [s/Any]
    task-opts :- {s/Any s/Any}]
   (stream task-name (merge {:twitter/keep-keys keep-keys}
                            task-opts))))
