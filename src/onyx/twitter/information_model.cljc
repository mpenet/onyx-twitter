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
      {:doc "Keys to keep in the tweet map after deconstructing the tweet. Defaults to `[:id :lang :text]`. `:all` will keep all the tweet's keys."
       :default [:id :lang :text]
       :type :vector}

      :twitter/track
      {:doc "An array of strings that you to get the Twitter API to track."
       :type :vector}

      :twitter/follow
      {:doc "An array of users ids that you to get the Twitter API to track."
       :type :vector}

      :twitter/filter-level
      {:doc "Filter level to pass to Twitter Streaming API."
       :type :keyword}

      :twitter/language
      {:doc "languages to pass to Twitter Streaming API."
       :type :vecrot}

      :twitter/locations
      {:doc "An array of coordinates to pass to Twitter Streaming API."
       :type :vector}}}}

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
     :twitter/track
     :twitter/follow
     :twitter/filter-level
     :twitter/language
     :twitter/locations]}})
