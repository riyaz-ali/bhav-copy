-- query to insert data into the equity table
INSERT INTO equity (exchange, type, trading_date, ticker, isin_code, open, high, low, close, last, previous_close)
VALUES (:exchange, :type, :trading_date, :ticker, :isin_code, :open, :high, :low, :close, :last, :previous_close);