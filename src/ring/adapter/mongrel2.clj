(ns ring.adapter.mongrel2
  (:import (org.zeromq ZMQ)
           (org.apache.commons.httpclient HttpStatus)
           (java.io File InputStream ByteArrayOutputStream
                    ByteArrayInputStream))
  (:use clojure.contrib.json
        [clojure.contrib.io :only (copy)]
        [clojure.contrib.except :only (throwf)]))

(defn- parse-netstring [s]
  (let [parts (.split s ":" 2)
        len (Integer/parseInt (first parts))]
    (apply str (take len (second parts)))))

(defn- make-netstring [s]
  (str (count s) ":" s ","))

(defn- split-m2-headers
  "Split a header map from mongrel2 into a map of mongrel2-specific headers
and a map of HTTP headers."
  [headers]
  (let [m2 #{"PATH" "METHOD" "VERSION" "URI" "PATTERN" "QUERY"}
        preds [#(contains? m2 %) #(not (contains? m2 %))]]
    (map #(apply hash-map (flatten
                           (for [[key val] headers :when (% key)]
                             [(.toLowerCase key) val])))
         preds)))

(defn- parse-m2-request [req]
  (let [[uuid client-id uri rest-str] (.split req " " 4)
        rest-str (.split rest-str ":" 2)
        header-len (Integer/parseInt (first rest-str))
        [headers body] (split-at header-len (second rest-str))
        headers (read-json (apply str headers) false)
        [m2-headers headers] (split-m2-headers headers)
        method ({"GET" :get, "HEAD" :head, "OPTIONS" :options, "PUT" :put,
`                 "POST" :post, "DELETE" :delete} (m2-headers "method"))
        body (parse-netstring (apply str (rest body)))
        body-stream (ByteArrayInputStream. (.getBytes body "US-ASCII"))
        request {:uuid uuid :client-id client-id :uri uri
                 :headers headers :request-method method
                 :scheme :http :server-name uuid :server-port 80
                 :remote-addr uuid}
        query (m2-headers "query")
        content-type (headers "content-type")
        request (if query (conj request [:query query]) request)
        request (if (not (empty? body))
                  (merge request {:body body-stream :content-length (count body)})
                  request)
        request (if content-type
                  (conj request [:content-type content-type])
                  request)]
    request))

(defn- make-http-reply [resp]
  (let [header-str (apply str (map #(str (first %) ": " (second %) "\r\n")
                                   (:headers resp)))
        status (:status resp)
        phrase (HttpStatus/getStatusText status)
        body (:body resp)
        body-stream (ByteArrayOutputStream.)
        out-stream (ByteArrayOutputStream.)]
    (cond (string? body) (copy body body-stream)
          (seq? body) (doseq [b body] (copy b body-stream))
          (instance? InputStream body) (do (copy body body-stream)
                                           (.close body))
          (instance? File body) (copy body body-stream)
          (nil? body) 1
          :else (throwf "Unrecognized body: %s" body))
    (copy (str "HTTP/1.1 " status " " phrase
               "\r\nContent-Length: " (.size body-stream)
               "\r\n" header-str "\r\n")
          out-stream)
    (copy (.toByteArray body-stream) out-stream)
    (.toByteArray out-stream)))

(defn run-mongrel2 [handler options]
  (let [ctx (ZMQ/context 1)
        sub (.socket ctx ZMQ/SUB)
        pub (.socket ctx ZMQ/PUB)
        recv-spec (or (:recv-spec options) "tcp://127.0.0.1:5566")
        send-spec (or (:send-spec options) "tcp://127.0.0.1:5565")]
    (.connect sub recv-spec)
    (.setsockopt sub ZMQ/SUBSCRIBE "")
    (.connect pub send-spec)
    (while (not (.isInterrupted (Thread/currentThread)))
      (let [req (parse-m2-request (String. (.recv sub 0)))
            ids (make-netstring (:client-id req))
            response (ByteArrayOutputStream.)]
        (if (not (= ((req :headers) "METHOD") "JSON"))
          (do
            (copy (str (:uuid req) " " ids " ") response)
            (copy (make-http-reply (handler req)) response)
            (.send pub (.toByteArray response) 0)))))))