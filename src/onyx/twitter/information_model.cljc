(ns onyx.twitter.information-model)

(def model
  {:catalog-entry
   {:onyx.plugin.twitter/consume-tweets
    {:summary "An input task to read tweets from the public Twitter API."
     :model
     {:twitter/consumer-key
      {:doc "The consumer API key."
       :type :string}

      :twitter/consumer-secret
      {:doc "The consumer API secret."
       :type :string}

      :twitter/access-token
      {:doc "The API access token."
       :type :string
       :optional? true}

      :twitter/access-secret
      {:doc "The API access secret."
       :type :string}

      :twitter/keep-keys
      {:doc "Keys to keep in the tweet map after deconstructing the POJO tweet. Defaults to `[:id :lang :text]`. `:all` will keep all the tweet's keys."
       :default [:id :lang :text]
       :type :vector}

      :twitter/filters
      {:doc "A map of filter parameters to be passed to Twitter API"
       :type :map}}}}

   :lifecycle-entry
   {:onyx.plugin.twitter/consume-tweets
    {:model
     [{:task.lifecycle/name :consume-tweets
       :lifecycle/calls :onyx.plugin.twitter/twitter-reader-calls}]}}

   :display-order
   {:onyx.plugin.twitter/consume-tweets
    [:twitter/consumer-key
     :twitter/consumer-secret
     :twitter/access-token
     :twitter/access-secret
     :twitter/keep-keys
     :twitter/filters]}})
