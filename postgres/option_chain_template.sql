CREATE TABLE IF NOT EXISTS data.{{ ticker }}_option_chains
(
    exp_date DATE,
    isCall BOOLEAN,
    lastTradeDate DATE, 
    strike FLOAT, 
    lastPrice FLOAT,
    bid FLOAT, 
    ask FLOAT,
    volume FLOAT, 
    openInterest INT,
    impliedVolatility FLOAT
);
