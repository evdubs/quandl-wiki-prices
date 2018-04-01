CREATE TABLE quandl.wiki_price
(
  act_symbol text NOT NULL,
  date date NOT NULL,
  open numeric,
  high numeric,
  low numeric,
  close numeric,
  volume bigint,
  ex_dividend numeric,
  split_ratio numeric,
  CONSTRAINT wiki_price_pkey PRIMARY KEY (act_symbol, date),
  CONSTRAINT wiki_price_act_symbol_pkey FOREIGN KEY (act_symbol)
      REFERENCES nasdaq.symbol (act_symbol) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION
);

