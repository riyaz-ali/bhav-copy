-- query to return latest recorded trading date by exchanges
SELECT MAX(trading_date) AS last_trading_date FROM equity WHERE exchange = :exchange