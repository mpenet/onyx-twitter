(ns onyx.plugin.twitter
  (:require
   [clojure.tools.logging :as log]
   [clojure.core.async :refer [<!! >!! chan close!]]
   [clojure.java.data :refer [from-java]]
   [cheshire.core :as json]
   [onyx.plugin simple-input
    [buffered-reader :as buffered-reader]])
  (:import
   (twitter4j Status RawStreamListener TwitterStream
              TwitterStreamFactory StatusJSONImpl
              FilterQuery)
   (twitter4j.conf Configuration ConfigurationBuilder)))

(defn config-with-password ^Configuration [consumer-key consumer-secret
                                           access-token access-secret]
  "Build a twitter4j configuration object with a username/password pair"
  (.build (doto  (ConfigurationBuilder.)
            (.setDebugEnabled true)
            (.setOAuthConsumerKey consumer-key)
            (.setOAuthConsumerSecret consumer-secret)
            (.setOAuthAccessToken access-token)
            (.setOAuthAccessTokenSecret access-secret))))

(defn status-listener
  "Implementation of twitter4j's StatusListener interface"
  [cb]
  (reify RawStreamListener
    (onMessage [this raw-message]
      (some-> raw-message (json/parse-string true) cb))
    (onException [this e]
      (log/error e))))

(defn get-twitter-stream ^TwitterStream [config]
  (-> (TwitterStreamFactory. ^Configuration config) .getInstance))

(defn add-stream-callback! [^TwitterStream stream cb]
  (let [tc (chan 1000)]
    (.addListener (cast TwitterStream stream)
                  (status-listener cb))
    ;; todo listen to endpoint from conf, sample is fun, but useless
    ;; in practice
    (.sample stream)))

(defn set-stream-filters
  [^TwitterStream stream params]
  (doto stream
    (.filter
     (let [fq (FilterQuery.)]
       (some->> params
                :follow
                (map long)
                (into-array Long/TYPE)
                (.follow fq))
       (some->> params
                :track
                (into-array java.lang.String)
                (.track fq))
       ;; add languages and other params
       fq))))

(defrecord ConsumeTweets [event task-map segment]
  onyx.plugin.simple-input/SimpleInput
  (start [this]
    (let [{:keys [twitter/consumer-key
                  twitter/consumer-secret
                  twitter/access-token
                  twitter/access-secret
                  twitter/keep-keys]} (:task-map this)
          configuration (config-with-password consumer-key consumer-secret
                                              access-token access-secret)
          twitter-stream (get-twitter-stream configuration)
          twitter-feed-ch (chan 1000)]
      (assert consumer-key ":twitter/consumer-key not specified")
      (assert consumer-secret ":twitter/consumer-secret not specified")
      (assert access-token ":twitter/access-token not specified")
      (assert access-secret ":twitter/access-secret not specified")
      (add-stream-callback! twitter-stream (fn [m] (>!! twitter-feed-ch m)))
      (assoc this
             :twitter-stream twitter-stream
             :twitter-feed-ch twitter-feed-ch)))
  (stop [{:keys [twitter-stream twitter-feed-ch]}]
    (do (close! twitter-feed-ch)
        (.shutdown ^TwitterStream twitter-stream)
        (.cleanUp  ^TwitterStream twitter-stream)))
  (checkpoint [this]
    -1)
  (segment-id[this]
    -1)
  (segment [this]
    segment)
  (next-state [{:keys [twitter-feed-ch task-map] :as this}]
    (let [keep-keys (get task-map :twitter/keep-keys)]
      (assoc this :segment (if (= :all keep-keys)
                             (<!! twitter-feed-ch)
                             (select-keys
                              (<!! twitter-feed-ch)
                              (or keep-keys [:id :text :lang]))))))
  (recover [this offset]
    this)
  (checkpoint-ack [this offset]
    this)
  (segment-complete! [this segment]))

(defn consume-tweets [{:keys [onyx.core/task-map] :as event}]
  (map->ConsumeTweets {:event event
                       :task-map task-map}))

(def twitter-reader-calls
  {:lifecycle/before-task-start
   (fn [event lifecycle]
     (let [plugin (get-in event [:onyx.core/task-map :onyx/plugin])]
       (case plugin
         :onyx.plugin.buffered-reader/new-buffered-input
         (buffered-reader/inject-buffered-reader event lifecycle))))
   :lifecycle/after-task-stop
   (fn [event lifecycle]
     (let [plugin (get-in event [:onyx.core/task-map :onyx/plugin])]
       (case plugin
         :onyx.plugin.buffered-reader/new-buffered-input
         (buffered-reader/close-buffered-reader event lifecycle))))})
