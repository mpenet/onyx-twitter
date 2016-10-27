(ns onyx.plugin.twitter
  (:require
   [clojure.tools.logging :as log]
   [clojure.core.async
    :refer [<!! >!! chan close! go]
    :as async]
   [clojure.java.data :refer [from-java]]
   [cheshire.core :as json]
   [onyx.plugin simple-input
    [buffered-reader :as buffered-reader]])
  (:import
   (twitter4j Status RawStreamListener TwitterStream
              TwitterStreamFactory StatusJSONImpl
              FilterQuery)
   (twitter4j.conf Configuration ConfigurationBuilder)))

(defn build-config
  [consumer-key consumer-secret access-token access-secret]
  (-> (doto (ConfigurationBuilder.)
        (.setDebugEnabled true)
        (.setOAuthConsumerKey consumer-key)
        (.setOAuthConsumerSecret consumer-secret)
        (.setOAuthAccessToken access-token)
        (.setOAuthAccessTokenSecret access-secret))
      .build))

(defn raw-stream-listener
  [on-message on-error]
  (reify RawStreamListener
    (onMessage [this raw-message]
      (some-> raw-message (json/parse-string true) on-message))
    (onException [this e]
      (on-error e))))

(defn ^TwitterStream get-stream [config]
  (-> (TwitterStreamFactory. ^Configuration config) .getInstance))

(defn add-listener! [^TwitterStream stream
                     on-message
                     on-error]
  (doto stream
    (.addListener (raw-stream-listener on-message on-error))))

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

(defmulti handle-signal (fn [stream t & _] t))

(defmethod handle-signal ::sample
  [^TwitterStream stream & _]
  (.sample stream))

(defmethod handle-signal ::filters
  [^TwitterStream stream _ params]
  (set-stream-filters stream params))

(defmethod handle-signal ::stop
  [^TwitterStream stream & _]
  (.shutdown stream)
  (.cleanUp  stream))

(defn start-stream-signal-handler!
  "Take a c.async channel and a stream instance that can receive
  signals that will control changes to apply to the current stream (ex
  start/stop/change/config. A signal is a par of signal-id and
  arguments"
  [^TwitterStream stream signal-ch]
  (async/go-loop []
    (when-let [signal (async/<! signal-ch)]
      (handle-signal signal)
      (recur))))

(defrecord ConsumeTweets [event task-map segment
                          signal-ch
                          ;; locals
                          feed-ch
                          ^TwitterStream stream]
  onyx.plugin.simple-input/SimpleInput
  (start [this]
    (let [{:keys [twitter/consumer-key
                  twitter/consumer-secret
                  twitter/access-token
                  twitter/access-secret
                  twitter/keep-keys]} (:task-map this)
          configuration (build-config consumer-key
                                      consumer-secret
                                      access-token
                                      access-secret)
          stream (get-stream configuration)
          feed-ch (chan 1000)]
      (assert consumer-key ":twitter/consumer-key not specified")
      (assert consumer-secret ":twitter/consumer-secret not specified")
      (assert access-token ":twitter/access-token not specified")
      (assert access-secret ":twitter/access-secret not specified")

      (add-listener! stream
                     (fn [m] (>!! feed-ch m))
                     #(log/error "Unhandled t4j RawStreamListener Handler error - " %))
      (start-stream-signal-handler! stream signal-ch)
      (assoc this
             :stream stream
             :feed-ch feed-ch
             :signal-ch signal-ch)))
  (stop [this]
    (close! feed-ch)
    (async/>! signal-ch ::stop)
    (close! signal-ch))
  (checkpoint [this]
    -1)
  (segment-id[this]
    -1)
  (segment [this]
    segment)
  (next-state [this]
    (let [keep-keys (:twitter/keep-keys task-map)]
      (assoc this :segment (if (= :all (:twitter/keep-keys task-map))
                             (<!! feed-ch)
                             (-> (<!! feed-ch)
                                 (select-keys (or keep-keys [:id :text :lang])))))))
  (recover [this offset] this)
  (checkpoint-ack [this offset] this)
  (segment-complete! [this segment]))

(defn consume-tweets [{:keys [onyx.core/task-map] :as event}]
  (map->ConsumeTweets
   {:event event
    :task-map task-map
    :signal-ch (chan)}))

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
