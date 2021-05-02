-- query to return latest recorded trading date by exchanges
SELECT exchange, MAX(trading_date) AS last_trading_date FROM equity GROUP BY exchange;