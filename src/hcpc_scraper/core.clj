(ns hcpc-scraper.core
  (require [org.httpkit.client :as http]
           [me.raynes.laser :as l]
           [clojure.data.csv :as csv]
           [clojure.java.io :as io]
           [clojure.pprint :refer [cl-format]]))

(def hcpc-url "http://www.hpc-uk.org/search-results/search-details/")

(defn pull-table [html]
  (l/select (l/parse html) (l/descendant-of (l/and (l/element= :table) (l/class= "resultsTable"))
                                            (l/or (l/element= :th) (l/element= :td)))))

(defn person-table [id]
  (http/get hcpc-url
            {:query-params {:ID id :profession "PYL"}}
            (fn [{:keys [body]}] (pull-table body))))

(defn person-record [table]
  (apply hash-map (mapcat :content table)))

(defn person-tables [l-id h-id]
  (doall (map person-table (range l-id h-id))))

(defn jitter [l-id h-id]
  (let [ms (+ 500 (rand 500))]
    (future (do (println (cl-format nil "Jittering for ~s ms before pulling IDs ~s to ~s" ms l-id h-id))
                (Thread/sleep ms)))))

; Add a random wait before each chunk is requested to not seem like DOS attacker
(defn person-records-chunk [l-id h-id chunk]
  (if-let [prs (seq (map person-record (filter seq (map deref (cons (jitter l-id h-id) (person-tables l-id h-id))))))]
    (lazy-cat prs (person-records-chunk (+ h-id 1) (+ h-id chunk) chunk))))

(defn write-csv [prs filename]
  (let [header (sort (keys (first prs)))]
    (with-open [out-file (io/writer filename)]
      (csv/write-csv out-file (cons header (map (fn [pr] (map pr header)) prs))))))

