# BhavCopy

Download historical stock data from BSE & NSE as [**`sqlite`**](http://sqlite.org) database :bar_chart: :chart:

-----------------------------------

**`bhavcopy`** allows you to download (and sync) historical stock price data from 
BSE <sup>[[1]](https://www.bseindia.com/markets/MarketInfo/BhavCopy.aspx)</sup> & NSE <sup>[[2]](https://www1.nseindia.com/products/content/equities/equities/archieve_eq.htm)</sup>. 
It makes use of the published bhavcopy records from those exchanges (published as `csv`) and allows you to download and merge them into a single, self-contained `sqlite` file.

## Usage

```shell
> bhav --help
Usage of bhav:
    --filename string   database file to sync (default "bhavcopy.db")
    --from timestamp    date to start syncing from (default 01-Jan-0001)
    --save-patch        save changeset to a patch file
    --verbose           enable verbose logging
```

The first time you invoke **`bhavcopy`** on a database file it'd start to sync data from Jan-1994 (for NSE) & Jan-2007 (for BSE). This _might_ cause your 
IP to be blacklisted temporarily by those exchanges (no one like a crawler :wink:). To preven that use `--until timestamp` (in conjunction with `--from`) 
and only download data for a quarter or half-year at a time. You can repeat this a few times to fetch all past data.

The database file contains the following tables:

- **`equity`**

```sql
CREATE TABLE equity
(
    exchange       TEXT NOT NULL CHECK (exchange IN ('bse', 'nse')),
    trading_date   TEXT NOT NULL CHECK (trading_date IS DATE(trading_date)),
    ticker         TEXT NOT NULL,
    type           TEXT NOT NULL,

    -- International Security Identification Number
    -- we keep this as nullable because not all securities have it and neither do older bhav copies
    isin_code      TEXT,

    -- a share's value related data
    open           FLOAT,
    high           FLOAT,
    low            FLOAT,
    close          FLOAT,
    last           FLOAT,
    previous_close FLOAT,

    -- we set a composite primary key on (exchange, date, ticker) tuple
    -- this allows us to ensure only unique values are recorded in the table
    -- for a given ticker from an exchange on a given date
    PRIMARY KEY (exchange, trading_date, ticker, type)
) WITHOUT ROWID;      
```


## License

The source code in this repository is provided under MIT License Copyright (c) 2020 Riyaz Ali

Refer to [LICENSE](./LICENSE) for full text.
