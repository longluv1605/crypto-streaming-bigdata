CREATE DATABASE IF NOT EXISTS crypto;

USE crypto;

CREATE TABLE IF NOT EXISTS bitcoin_raw (
    timestamp DATETIME NOT NULL PRIMARY KEY,
    open DOUBLE NOT NULL,
    high DOUBLE NOT NULL,
    low DOUBLE NOT NULL,
    close DOUBLE NOT NULL,
    volume DOUBLE NOT NULL
);

CREATE TABLE IF NOT EXISTS bitcoin_processed (
    timestamp DATETIME NOT NULL PRIMARY KEY,
    open DOUBLE NOT NULL,
    high DOUBLE NOT NULL,
    low DOUBLE NOT NULL,
    close DOUBLE NOT NULL,
    volume DOUBLE NOT NULL,
    price_avg DOUBLE NOT NULL,
    price_change DOUBLE NOT NULL,
    moving_avg DOUBLE,
    volatility DOUBLE NOT NULL,
    future_close DOUBLE NOT NULL
);
