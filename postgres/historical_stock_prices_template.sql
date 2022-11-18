CREATE TABLE IF NOT EXISTS rahjput_data.{{ ticker }}_historical_stock_prices 
(
    date TIMESTAMP PRIMARY KEY,
    open FLOAT,
    high FLOAT, 
    low FLOAT, 
    Close FLOAT,
    Adj_Close FLOAT, 
    Volume FLOAT
);