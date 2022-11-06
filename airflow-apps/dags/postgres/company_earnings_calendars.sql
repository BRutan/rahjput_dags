CREATE TABLE IF NOT EXISTS data.company_earnings_calendars
(
    company_id SEQUENCE REFERENCES company_info(company_id),
    earnings_date DATE
);