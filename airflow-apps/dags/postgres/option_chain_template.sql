CREATE TABLE IF NOT EXISTS data.{{ ticker }}_option_chains AS
(
    exp_date DATE,
    strike FLOAT,
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
