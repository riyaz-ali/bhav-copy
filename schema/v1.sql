-- This file contains the default schema for the sqlite database

-- Table 'equity' stores historical stock price information
-- about the equity segment traded on BSE and NSE
CREATE TABLE equity
(
    exchange       TEXT NOT NULL CHECK (exchange IN ('bse', 'nse')),
    trading_date   TEXT NOT NULL CHECK (trading_date IS DATE(trading_date)), -- see: https://stackoverflow.com/a/64054628/6611700
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