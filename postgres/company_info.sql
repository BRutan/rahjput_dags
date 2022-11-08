CREATE TABLE data.Company_Info AS
(
    company_id SERIAL PRIMARY KEY,
    ticker varchar,
    industry varchar,
    market_cap float
);