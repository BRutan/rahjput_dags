CREATE TABLE IF NOT EXISTS rahjput_data.company_info
(
    company_id SERIAL REFERENCES data.tickers_to_track(company_id),
    ticker varchar,
    industry varchar,
    market_cap float
);