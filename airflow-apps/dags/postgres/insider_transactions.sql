CREATE TABLE IF NOT EXISTS data.insider_transactions AS
(
    companyid SERIAL REFERENCES data.company_info,
    name VARCHAR,
    share INT,
    change INT,
    filingDate DATE,
    transactionDate DATE,
    transactionCode VARCHAR,
    transactionPrice FLOAT
);