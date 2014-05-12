(ns hcpc-scraper.core
  (require [org.httpkit.client :as http]
           [me.raynes.laser :as l]
           [clojure.data.csv :as csv]
           [clojure.java.io :as io]
           [clojure.pprint :refer [cl-format]]))

(def hcpc-search-url "http://www.hpc-uk.org/search-results/")

(def hcpc-url "http://www.hpc-uk.org/search-results/search-details/")

(defn pull-table [html]
  (l/select (l/parse html) (l/descendant-of (l/and (l/element= :table) (l/class= "resultsTable"))
                                            (l/or (l/element= :th) (l/element= :td)))))

(defn person-table [id p]
  (http/get hcpc-url
            {:query-params {:ID id :profession (str p)}}
            (fn [{:keys [body]}] (pull-table body))))

(defn pull-professions [html]
  (apply hash-map (mapcat (juxt (comp keyword :value :attrs) (comp first :content))
                          (filter (comp seq :value :attrs)
                                  (l/select (l/parse html) (l/child-of (l/id= "ctl00_searchProfession")
                                                                       (l/element= :option)))))))
(def professions
  (memoize (fn []
             @(http/get hcpc-search-url
                        {}
                        (fn [{:keys [body]}] (pull-professions body))))))

(defn person-record [table]
  (apply hash-map (mapcat :content table)))

(defn person-tables [l-id h-id p]
  (doall (map person-table (range l-id h-id) p)))

(defn jitter [l-id h-id]
  (let [ms (+ 500 (rand 500))]
    (future (do (println (cl-format nil "Jittering for ~s ms before pulling IDs ~s to ~s" ms l-id h-id))
                (Thread/sleep ms)))))

(defn person-records-chunk [start-id end-id p]
  (seq (map person-record (filter seq (map deref (cons (jitter start-id end-id)
                                                       (person-tables start-id end-id p)))))))

(defn some-record-at [id]
  (keys (professions)))

(defn verify-records-end [end-id]
  )

; Add a random wait before each chunk is requested to not seem like DOS attacker
(defn person-records [start-id chunk-size p]
  (let [end-id (+ start-id chunk-size -1)]
    (if-let [prs (person-records-chunk start-id end-id p)]
      (lazy-cat prs (person-records (+ end-id 1) (+ end-id chunk-size) chunk-size))
      )))

(defn write-csv [prs filename]
  (let [header (sort (keys (first prs)))]
    (with-open [out-file (io/writer filename)]
      (csv/write-csv out-file (cons header (map (fn [pr] (map pr header)) prs))))))


