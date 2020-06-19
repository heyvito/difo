(ns difo.raft-test
  (:require [clojure.test :refer :all]
            [difo.raft :refer :all]
            [difo.helpers :refer [import-private-functions
                                  wait-flag
                                  killing-stdout
                                  with-timeout
                                  set-flag-and-noop]]
            [difo.threading :refer [take!!]]
            [difo.fake-redis :as redis]
            [clojure.core.async :as async]))

(import-private-functions difo.raft milliseconds
                          message
                          announce!
                          valid-term?
                          invoke-callback
                          become-follower
                          become-leader
                          assert-heartbeat
                          on-heartbeat
                          determine-quorum
                          schedule-timeout
                          start-election
                          on-heartbeat-timeout
                          heartbeat-timeout
                          on-election
                          on-election-timeout
                          on-receive-vote
                          message-handler
                          schedule-register)

(defmacro test-inflections [forms in out]
  (let [forms (map (fn [x]
                     `(is (= ~out (milliseconds ~in ~x)))) forms)]
    `(do ~@forms)))

(deftest test-milliseconds-conversion
  (testing "millisecond"
    (test-inflections [:millisecond :milliseconds] 1 1))
  (testing "second"
    (test-inflections [:second :seconds] 1 1000))
  (testing "minute"
    (test-inflections [:minute :minutes] 1 60000))
  (testing "hour"
    (test-inflections [:hour :hours] 1 3600000)))

(def node (new-node :bar))

(deftest test-message
  (is (= {:type :foo :from :bar :test true}
         (message node :foo :test true))))

(deftest test-announce
  (redis/with-fake-redis
    (announce! :foo)
    (is (= (first @redis/published-raft) :foo))))

(deftest test-valid-term
  (testing "with a valid term"
    (is (valid-term? node {:term 0})))
  (testing "with a past term"
    (is (not (valid-term? node {:term -1})))))

(deftest test-invoke-callback
  (let [node (new-node :foo)
        called? (atom false)]
    (set-callback node :test (fn [n]
                               (when (= n node)
                                 (reset! called? true))))
    (invoke-callback node :test)
    (is @called?)))

(deftest test-become-follower
  (testing "with an invalid term"
    (let [node (new-node :foo)
          heartbeat {:from :bar :term -1}]
      (is (nil? @(:leader node)))
      (become-follower node heartbeat)
      (is (nil? @(:leader node)))))

  (testing "with a valid term"
    (let [node (new-node :foo)
          heartbeat {:from :bar :term 1}]
      (is (nil? @(:leader node)))
      (become-follower node heartbeat)
      (is (= :bar @(:leader node)))))

  (testing "callbacks"
    (let [node (new-node :foo)
          heartbeat {:from :bar :term 1}
          called? (atom false)]
      (reset! (:status node) :leader)
      (set-callback node :resigned-leader (set-flag-and-noop called?))
      (is (= "[Raft] Became follower of :bar\n"
             (with-out-str (become-follower node heartbeat))))
      (is (= :follower @(:status node)))
      (is @called?))))

(deftest test-become-leader
  (let [node (new-node :foo)
        callback-called? (atom false)
        heartbeat-announced? (atom false)
        fake-announce (fn [_]
                        (reset! heartbeat-announced? true)
                        (reset! (:status node) :follower))]
    (set-callback node :became-leader (set-flag-and-noop callback-called?))
    (reset! (:status node) :candidate)
    (redis/with-fake-redis
      (with-redefs-fn {#'difo.raft/announce! fake-announce}
        #(do (is (= "[Raft] Became leader\n"
                    (with-out-str (become-leader node))))
             (is @callback-called?)
             (wait-flag heartbeat-announced? 2000))))))

(deftest test-assert-heartbeat
  (testing "as a follower"
    (let [node (new-node :foo)
          hb {:term 1}]
      (assert-heartbeat node hb)
      (is (zero? (:current @(:term node))))))
  (testing "as a leader and an invalid hb"
    (let [node (new-node :foo)
          hb {:term -1}]
      (reset! (:status node) :leader)
      (assert-heartbeat node hb)
      (is (zero? (:current @(:term node))))))
  (testing "as a leader and a valid hb"
    (let [node (new-node :foo)
          hb {:term 1}
          became-follower? (atom false)]
      (reset! (:status node) :leader)
      (with-redefs-fn {#'difo.raft/become-follower (set-flag-and-noop became-follower?)}
        #(is (= "[Raft] Stepping down due to hb: {:term 1} (Current [term:0][votes:0])\n"
                (with-out-str (assert-heartbeat node hb)))))
      (is @became-follower?))))

(deftest test-on-heartbeat
  (testing "as a follower"
    (let [node (new-node :foo)
          hb {:hb true}
          became-follower? (atom false)]
      (with-redefs-fn {#'difo.raft/become-follower (fn [_ received-hb]
                                                     (is (= hb received-hb))
                                                     (reset! became-follower? true))}
        #(on-heartbeat node hb))
      (is @became-follower?)))
  (testing "as a candidate"
    (let [node (new-node :foo)
          hb {:term 2}
          became-follower? (atom false)]
      (reset! (:status node) :candidate)
      (with-redefs-fn {#'difo.raft/become-follower (fn [_ received-hb]
                                                     (is (= hb received-hb))
                                                     (reset! became-follower? true))}
        #(killing-stdout (on-heartbeat node hb)))
      (is @became-follower?)))
  (testing "as a leader"
    (let [node (new-node :foo)
          hb {:term 2}
          became-follower? (atom false)]
      (reset! (:status node) :leader)
      (with-redefs-fn {#'difo.raft/become-follower (fn [_ received-hb]
                                                     (is (= hb received-hb))
                                                     (reset! became-follower? true))}
        #(killing-stdout (on-heartbeat node hb)))
      (is @became-follower?))))

(deftest test-determine-quorum
  (redis/with-fake-redis
    (redis/set-fake-processes! ["difo:processes:foo" "difo:processes:bar" "difo:processes:baz"])
    (let [node (new-node "foo")]
      (is (= 1 (determine-quorum node))))))

(deftest test-schedule-timeout
  (let [node (new-node :foo)
        callback-chan (get-in node [:channels :heartbeat])]
    (schedule-timeout node :heartbeat 200)
    (with-timeout 500 (is (= {} (take!! callback-chan))))))

(deftest test-start-election
  (testing "without quorum"
    (let [node (new-node "foo")
          became-leader? (atom false)]
      (redis/with-fake-redis
        (with-redefs-fn {#'difo.raft/become-leader (set-flag-and-noop became-leader?)}
          #(start-election node))
        (is (= 1 (:current @(:term node))))
        (is (zero? (:quorum @(:term node))))
        (is @became-leader?))))
  (testing "with quorum"
    (let [node (new-node "foo")
          announced? (atom false)
          scheduled-timeout? (atom false)]
      (redis/with-fake-redis
        (redis/set-fake-processes! ["difo:processes:foo" "difo:processes:bar" "difo:processes:baz"])
        (with-redefs-fn {#'difo.raft/schedule-timeout (fn [_ kind _]
                                                        (when (= kind :election-timeout)
                                                          (reset! scheduled-timeout? true)))}
          #(start-election node))
        (is @scheduled-timeout?)
        (is (= {:type :election
                :from "foo"
                :term 1}
               (first @redis/published-raft)))))))

(deftest test-on-heartbeat-timeout
  (testing "as a follower"
    (let [node (new-node :foo)
          started-election? (atom false)
          rescheduled? (atom false)]
      (reset! (:last-heartbeat node) (- (System/currentTimeMillis) @heartbeat-timeout 10))
      (with-redefs-fn {#'difo.raft/start-election   (set-flag-and-noop started-election?)
                       #'difo.raft/schedule-timeout (fn [_ type _]
                                                      (when (= type :heartbeat-timeout)
                                                        (reset! rescheduled? true)))}
        #(on-heartbeat-timeout node))
      (is @started-election?)
      (is @rescheduled?)))
  (testing "as a leader"
    (let [node (new-node :foo)
          started-election? (atom false)
          rescheduled? (atom false)]
      (reset! (:status node) :leader)
      (reset! (:last-heartbeat node) (- (System/currentTimeMillis) @heartbeat-timeout 10))
      (with-redefs-fn {#'difo.raft/start-election   (set-flag-and-noop started-election?)
                       #'difo.raft/schedule-timeout (fn [_ type _]
                                                      (when (= type :heartbeat-timeout)
                                                        (reset! rescheduled? true)))}
        #(on-heartbeat-timeout node))
      (is (not @started-election?))
      (is @rescheduled?))))

(deftest test-on-election
  (testing "with an invalid term"
    (let [node (new-node :foo)
          msg {:term -1}]
      (on-election node msg)
      (is (not @(:voted? node)))
      (is (empty? @redis/published-raft))))
  (testing "after voting on current term"
    (let [node (new-node :foo)
          msg {:term 2}]
      (reset! (:voted? node) true)
      (on-election node msg)
      (is (empty? @redis/published-raft))))
  (testing "voting"
    (let [node (new-node :foo)
          msg {:term 2}]
      (redis/with-fake-redis
        (on-election node msg)
        (is @(:voted? node))
        (is (not (empty? @redis/published-raft)))))))

(deftest test-on-election-timeout
  (testing "without enough quorum"
    (let [node (new-node :foo)
          became-leader? (atom false)]
      (reset! (:term node) {:votes 1 :quorum 2})
      (reset! (:status node) :candidate)
      (with-redefs-fn {#'difo.raft/become-leader (set-flag-and-noop became-leader?)}
        #(on-election-timeout node))
      (is (not @became-leader?))))
  (testing "with enough quorum"
    (let [node (new-node :foo)
          became-leader? (atom false)]
      (reset! (:term node) {:votes 3 :quorum 2})
      (with-redefs-fn {#'difo.raft/become-leader (set-flag-and-noop became-leader?)}
        #(on-election-timeout node))
      (is @became-leader?))))

(deftest test-on-receive-vote
  (testing "while not a candidate"
    (let [node (new-node :foo)
          vote {:name :foo}]
      (on-receive-vote node vote)
      (is (= 0 (:votes @(:term node))))))
  (testing "when vote is not to node"
    (let [node (new-node :foo)
          vote {:vote :bar}]
      (reset! (:status node) :candidate)
      (on-receive-vote node vote)
      (is (= 0 (:votes @(:term node))))))
  (testing "when vote is for node and it is a candidate"
    (let [node (new-node :foo)
          vote {:vote :foo}]
      (reset! (:status node) :candidate)
      (on-receive-vote node vote)
      (is (= 1 (:votes @(:term node)))))))

(deftest test-message-handler
  (testing "from other node"
    (let [initial-node (new-node :foo)
          ;; Channels of nodes are initialized without a buffer. Here we will just
          ;; replace them with buffered ones so we can test in two parts.
          node (assoc initial-node :channels (merge (:channels initial-node)
                                                    {:heartbeat (async/chan 1)
                                                     :election  (async/chan 1)
                                                     :vote      (async/chan 1)}))
          hb-msg {:type :heartbeat :from :bar}
          el-msg {:type :election :from :bar}
          vt-msg {:type :vote :from :bar}
          handle (message-handler node)]
      (handle hb-msg)
      (handle el-msg)
      (handle vt-msg)
      (let [{:keys [heartbeat election vote]} (:channels node)]
        (is (= hb-msg (take!! heartbeat)))
        (is (= el-msg (take!! election)))
        (is (= vt-msg (take!! vote))))))
  (testing "from itself"
    (let [node (new-node :foo)
          enqueued? (atom false)
          msg {:type :heartbeat :from :foo}
          handle (message-handler node)]
      (with-redefs-fn {#'async/put! (set-flag-and-noop enqueued?)}
        #(handle msg))
      (is (not @enqueued?)))))

(deftest test-schedule-register
  (let [node (new-node :foo)
        counter (atom 0)
        flag (atom false)]
    (with-redefs-fn {#'difo.raft/milliseconds      (fn [_ _] 100)
                     #'difo.redis/register-process (fn [_]
                                                     (swap! counter inc)
                                                     (when (= @counter 2)
                                                       (reset! (:running? node) false)
                                                       (reset! flag true)))}
      #(do (schedule-register node)
           (wait-flag flag 1000)
           (is (= @counter 2))))))

(deftest test-stop
  (let [node (assoc (new-node :foo) :pub-sub (atom :pub))
        raft-stopped? (atom false)
        unregistered? (atom false)]
    (with-redefs-fn {#'difo.redis/unregister-process (set-flag-and-noop unregistered?)
                     #'difo.redis/stop-raft-listener (set-flag-and-noop raft-stopped?)}
      #(do
         (stop node)
         (let [{:keys [running? pub-sub channels]} node]
           (is (not @running?))
           (is (nil? @pub-sub))
           (is @raft-stopped?)
           (is @unregistered?)
           (is (every? nil? (map take!! (vals channels)))))))))

(deftest test-run
  (let [node (new-node :foo)
        listener-created? (atom false)
        scheduled-heartbeat? (atom false)
        scheduled-register? (atom false)
        received-heartbeat? (atom false)
        received-heartbeat-timeout? (atom false)
        received-election? (atom false)
        received-election-timeout? (atom false)
        received-vote? (atom false)
        result-chan (async/thread
                      (with-redefs-fn {#'difo.redis/new-raft-listener   (set-flag-and-noop listener-created?)
                                       #'difo.raft/schedule-timeout     (set-flag-and-noop scheduled-heartbeat?)
                                       #'difo.raft/schedule-register    (set-flag-and-noop scheduled-register?)
                                       #'difo.raft/on-heartbeat         (set-flag-and-noop received-heartbeat?)
                                       #'difo.raft/on-heartbeat-timeout (set-flag-and-noop received-heartbeat-timeout?)
                                       #'difo.raft/on-election          (set-flag-and-noop received-election?)
                                       #'difo.raft/on-election-timeout  (set-flag-and-noop received-election-timeout?)
                                       #'difo.raft/on-receive-vote      (set-flag-and-noop received-vote?)}
                        #(run node)))]
    (let [{:keys [heartbeat heartbeat-timeout
                  election election-timeout
                  vote stop]} (:channels node)]
      (async/put! heartbeat {})
      (wait-flag received-heartbeat? 1000)
      (async/put! heartbeat-timeout {})
      (wait-flag received-heartbeat-timeout? 1000)
      (async/put! election {})
      (wait-flag received-election? 1000)
      (async/put! election-timeout {})
      (wait-flag received-election-timeout? 1000)
      (async/put! vote {})
      (wait-flag received-vote? 1000)
      (reset! (:running? node) false)
      (async/put! stop {}))
    (wait-flag listener-created? 1000)
    (wait-flag scheduled-heartbeat? 1000)
    (wait-flag scheduled-register? 1000)
    (take!! result-chan)))

(deftest test-unset-callback
  (let [node (new-node :foo)]
    (set-callback node :foo :bar)
    (is (= node (unset-callback node :foo)))
    (is (not (contains? @(:callbacks node) :foo)))))
