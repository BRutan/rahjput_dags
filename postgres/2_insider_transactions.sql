CREATE TABLE IF NOT EXISTS rahjput_data.insider_transactions
(
    company_id SERIAL REFERENCES rahjput_data.tickers_to_track(company_id),
    name VARCHAR,
    share INT,
    change INT,
    filingDate DATE,
    transactionDate DATE,
    transactionCode VARCHAR,
    transactionPrice FLOAT
);