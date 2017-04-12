(ns onyx.plugin.twitter
  (:require [clojure.core.async :refer [<!! >!! chan close! poll!]]
            [clojure.tools.logging :as log]
            [onyx.plugin.protocols.plugin :as p]
            [onyx.plugin.protocols.input :as i]
            [clojure.java.data :refer [from-java]]
            [cheshire.core :as json])
  (:import [twitter4j Status RawStreamListener TwitterStream
            TwitterStreamFactory StatusJSONImpl FilterQuery]
           [twitter4j.conf Configuration ConfigurationBuilder]))

(defn config-with-password ^Configuration
  [consumer-key consumer-secret
   access-token access-secret]
  "Build a twitter4j configuration object with a username/password pair"
  (.build (doto  (ConfigurationBuilder.)
            (.setOAuthConsumerKey consumer-key)
            (.setOAuthConsumerSecret consumer-secret)
            (.setOAuthAccessToken access-token)
            (.setOAuthAccessTokenSecret access-secret))))

(defn raw-stream-listener
  [on-message on-error]
  (reify RawStreamListener
    (onMessage [this raw-message]
      (some-> raw-message (json/parse-string true) on-message))
    (onException [this e]
      (on-error e))))

(defn ^TwitterStream get-twitter-stream [config]
  (-> (TwitterStreamFactory. ^Configuration config) .getInstance))


(defn set-stream-filters!
  [^TwitterStream stream track follow language locations filter-level]
  (doto stream
    (.filter (cond-> (FilterQuery.)
               (seq follow)
               (.follow (into-array Long/TYPE (map long follow)))

               (seq track)
               (.track (into-array java.lang.String track))

               (some? filter-level)
               (.filterLevel filter-level)

               (seq language)
               (.language (into-array java.lang.String language))

               (seq locations)
               (.locations (into-array (map double-array locations)))))))

(defn add-listener! [^TwitterStream stream
                     on-message
                     on-error]
  (doto stream
    (.addListener (raw-stream-listener on-message on-error))))

(defrecord ConsumeTweets [event task-map segment
                          ;; locals
                          twitter-feed-ch
                          ^TwitterStream stream]
  p/Plugin
  (start [this event]
    (let [{:keys [twitter/consumer-key
                  twitter/consumer-secret
                  twitter/access-token
                  twitter/access-secret
                  twitter/keep-keys
                  twitter/track
                  twitter/follow
                  twitter/language
                  twitter/filter-level
                  twitter/locations]} (:task-map this)
          configuration (config-with-password consumer-key consumer-secret
                                              access-token access-secret)
          twitter-stream (get-twitter-stream configuration)
          twitter-feed-ch (chan 1000)]
      (assert consumer-key ":twitter/consumer-key not specified")
      (assert consumer-secret ":twitter/consumer-secret not specified")
      (assert access-token ":twitter/access-token not specified")
      (assert access-secret ":twitter/access-secret not specified")
      (add-listener! twitter-stream
                     (fn [m] (>!! twitter-feed-ch m))
                     #(log/error "Unhandled t4j RawStreamListener Handler error - " %))
      (set-stream-filters! twitter-stream
                           track follow language filter-level locations)
      (assoc this
             :twitter-stream twitter-stream
             :twitter-feed-ch twitter-feed-ch)))
  (stop [this event]
    (.cleanUp ^TwitterStream (:twitter-stream this))
    (close! twitter-feed-ch)
    (close! (:twitter-feed-ch this))
    this)

  i/Input
  (checkpoint [this])

  (recover! [this replica-version checkpoint]
    this)

  (synced? [this epoch]
    true)

  (checkpointed! [this epoch])

  (poll! [this _]
    (let [keep-keys (get task-map :twitter/keep-keys)
          tweet (poll! twitter-feed-ch)]
      (when tweet
        (if (= :all keep-keys)
          tweet
          (select-keys tweet
                       (or keep-keys [:id :text :lang])))))))

(defn consume-tweets [{:keys [onyx.core/task-map] :as event}]
  (map->ConsumeTweets {:event event
                       :task-map task-map}))

(def twitter-reader-calls
  {})
