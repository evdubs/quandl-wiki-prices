# quandl-wiki-prices
These Racket programs will download the Quandl WIKI/PRICES datatable CSV files and insert the data into a PostgreSQL database. The intended usage is:

```
$ racket extract.rkt
$ racket transform-load.rkt
```

The provided schema.sql file shows the expected schema within the target PostgreSQL instance. This process assumes you can write to a 
/var/tmp/quandl/wiki-prices folder. This process also assumes you have loaded your database with the NASDAQ symbol file information.
This data is provided by the [nasdaq-symbols](https://github.com/evdubs/nasdaq-symbols) project.

Finally, you will need to provide your Quandl API key. Once you create an account with Quandl, you can find this key in Account Settings > API Key.

