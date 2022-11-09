CREATE TABLE IF NOT EXISTS rahjput_data.{{ ticker }}_option_chains
(
    expirationDate DATE,
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