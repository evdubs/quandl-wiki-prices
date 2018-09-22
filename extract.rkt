#lang racket/base

(require db
         net/url
         racket/cmdline
         racket/file
         racket/list
         racket/port
         racket/string
         srfi/19 ; Time Data Types and Procedures
         tasks
         threading)

(define (download-prices symbol start-date end-date api-key)
  (make-directory* (string-append "/var/tmp/quandl/wiki-prices/" (date->string (current-date) "~1")))
  (call-with-output-file (string-append "/var/tmp/quandl/wiki-prices/" (date->string (current-date) "~1") "/"
                                        (string-replace symbol  "_" ".") ".prices.csv")
    (λ (out) (~> (string-append "https://www.quandl.com/api/v3/datatables/WIKI/PRICES.csv?ticker=" symbol
                                "&api_key=" api-key "&date.gte=" (date->string start-date "~1") "&date.lte=" (date->string end-date "~1"))
                 (string->url _)
                 (get-pure-port _)
                 (copy-port _ out)))
    #:exists 'replace))

(define start-date (make-parameter (current-date)))

(define end-date (make-parameter (current-date)))

(define api-key (make-parameter ""))

(define db-user (make-parameter "user"))

(define db-name (make-parameter "local"))

(define db-pass (make-parameter ""))

(command-line
 #:program "racket extract.rkt"
 #:once-each
 [("-a" "--api-key") ak
                     "Quandl API key"
                     (api-key ak)]
 [("-e" "--end-date") end
                      "Final date for history retrieval. Defaults to today"
                      (end-date (string->date end "~Y-~m-~d"))]
 [("-n" "--db-name") name
                     "Database name. Defaults to 'local'"
                     (db-name name)]
 [("-p" "--db-pass") password
                     "Database password"
                     (db-pass password)]
 [("-s" "--start-date") start
                        "Earliest date for history retrieval. Defaults to today"
                        (start-date (string->date start "~Y-~m-~d"))]
 [("-u" "--db-user") user
                     "Database user name. Defaults to 'user'"
                     (db-user user)])

(define dbc (postgresql-connect #:user (db-user) #:database (db-name) #:password (db-pass)))

(define symbols (query-list dbc "
select
  replace(act_symbol, '.', '_') as act_symbol
from
  nasdaq.symbol
where
  is_etf = false and
  is_test_issue = false and
  is_next_shares = false and
  security_name !~ 'ETN' and
  nasdaq_symbol !~ '[-\\$\\+\\*#!@%\\^=~]' and
  case when nasdaq_symbol ~ '[A-Z]{4}[L-Z]'
    then security_name !~ '(Note|Preferred|Right|Unit|Warrant)'
    else true
  end
order by
  act_symbol;
"))

(disconnect dbc)

(define delay-interval 5)

(define delays (map (λ (x) (* delay-interval x)) (range 0 (length symbols))))

(with-task-server (for-each (λ (l) (schedule-delayed-task (λ () (download-prices (first l) (start-date) (end-date) (api-key)))
                                                          (second l)))
                            (map list symbols delays))
  ; add a final task that will halt the task server
  (schedule-delayed-task (λ () (schedule-stop-task)) (* delay-interval (length delays)))
  (run-tasks))
