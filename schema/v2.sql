-- This migration adds additional index on equity.ticker to allow more efficient
-- queries by filtering on equity.ticker = <value>

CREATE INDEX equity_ticker ON equity (ticker);