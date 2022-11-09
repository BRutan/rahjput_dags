CREATE TABLE IF NOT EXISTS insider_transactions
(
    company_id SERIAL REFERENCES tickers_to_track(company_id),
    name VARCHAR,
    share INT,
    change INT,
    filingDate DATE,
    transactionDate DATE,
    transactionCode VARCHAR,
    transactionPrice FLOAT
);