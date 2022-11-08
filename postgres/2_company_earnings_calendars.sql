CREATE TABLE IF NOT EXISTS data.company_earnings_calendars
(
    company_id SERIAL REFERENCES data.tickers_to_track(company_id),
    earnings_date DATE
);