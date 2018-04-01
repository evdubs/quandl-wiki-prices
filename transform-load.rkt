#lang racket

(require db)
(require racket/cmdline)
(require srfi/19) ; Time Data Types and Procedures
(require threading)

(struct price-entry
  (ticker
   date
   open
   high
   low
   close
   volume
   ex-dividend
   split-ratio
   adj-open
   adj-high
   adj-low
   adj-close
   adj-volume))

(define base-folder (make-parameter "/var/tmp/quandl/wiki-prices"))

(define folder-date (make-parameter (current-date)))

(define db-user (make-parameter "user"))

(define db-name (make-parameter "local"))

(define db-pass (make-parameter ""))

(command-line
 #:program "racket transform-load.rkt"
 #:once-each
 [("-b" "--base-folder") folder
                         "Quandl wiki prices base folder. Defaults to /var/tmp/quandl/wiki-prices"
                         (base-folder folder)]
 [("-d" "--folder-date") date
                       "Quandl wiki prices folder date. Defaults to today"
                       (folder-date (string->date date "~Y-~m-~d"))]
 [("-n" "--db-name") name
                     "Database name. Defaults to 'local'"
                     (db-name name)]
 [("-p" "--db-pass") password
                     "Database password"
                     (db-pass password)]
 [("-u" "--db-user") user
                     "Database user name. Defaults to 'user'"
                     (db-user user)])

(define dbc (postgresql-connect #:user (db-user) #:database (db-name) #:password (db-pass)))

(parameterize ([current-directory (string-append (base-folder) "/" (date->string (folder-date) "~1") "/")])
  (for ([p (sequence-filter (λ (p) (string-contains? (path->string p) ".prices.csv")) (in-directory))])
    (let ([file-name (string-append (base-folder) "/" (date->string (folder-date) "~1") "/" (path->string p))]
          [ticker-symbol (string-replace (path->string p) ".prices.csv" "")])
      (call-with-input-file file-name
        (λ (in)
          (with-handlers ([exn:fail? (λ (e) (displayln (string-append "Failed to process "
                                                                      ticker-symbol
                                                                      " for date "
                                                                      (date->string (folder-date) "~1")))
                                       (displayln ((error-value->string-handler) e 1000))
                                       (rollback-transaction dbc))])
            (~> (in-lines in)
                (sequence-map (λ (el) (string-split el ",")) _)
                (sequence-filter (λ (el) (not (equal? "ticker" (first el)))) _)
                (sequence-for-each (λ (el)
                                     (start-transaction dbc)
                                     (let ([px (apply price-entry el)])
                                       (query-exec dbc "
insert into quandl.wiki_price (
  act_symbol,
  date,
  open,
  high,
  low,
  close,
  volume,
  ex_dividend,
  split_ratio
) values (
  $1,
  $2::text::date,
  $3::text::numeric,
  $4::text::numeric,
  $5::text::numeric,
  $6::text::numeric,
  $7::text::numeric::bigint,
  $8::text::numeric,
  $9::text::numeric
) on conflict (act_symbol, date) do nothing;
"
                                                   ticker-symbol
                                                   (price-entry-date px)
                                                   (price-entry-open px)
                                                   (price-entry-high px)
                                                   (price-entry-low px)
                                                   (price-entry-close px)
                                                   (price-entry-volume px)
                                                   (price-entry-ex-dividend px)
                                                   (price-entry-split-ratio px))
                                       (commit-transaction dbc))) _))))))))

(disconnect dbc)
