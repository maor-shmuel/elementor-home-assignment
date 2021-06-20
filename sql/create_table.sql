-- DROP TABLE IF EXISTS sites;

CREATE TABLE IF NOT EXISTS sites (
    id integer PRIMARY KEY AUTOINCREMENT,
    url text NOT NULL,
    site_risk text,
    counts jsonb,
    classifications jsonb,
    ingest_timestamp timestamp DEFAULT current_timestamp);
