CREATE TABLE IF NOT EXISTS rahjput_data.company_info
(
    company_id SERIAL REFERENCES rahjput_data.tickers_to_track(company_id),
    industry varchar,
    market_cap float
);