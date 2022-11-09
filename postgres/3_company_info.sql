CREATE TABLE IF NOT EXISTS company_info
(
    company_id SERIAL REFERENCES tickers_to_track(company_id),
    ticker varchar,
    industry varchar,
    market_cap float
);