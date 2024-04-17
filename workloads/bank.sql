CREATE DATABASE IF NOT EXISTS bank;

USE bank;

CREATE TABLE IF NOT EXISTS ref_data (
    my_sequence INT PRIMARY KEY,
    my_costant VARCHAR,
    my_uuid UUID,
    my_choice STRING,
    my_integer INT,
    my_float FLOAT ARRAY,
    my_decimal DECIMAL(15, 4),
    my_timestamp TIMESTAMP,
    my_date DATE,
    my_time TIME,
    my_bit BIT(10),
    my_bytes BYTES,
    my_string VARCHAR[],
    my_bool BOOL,
    my_json JSONB
);

CREATE TABLE IF NOT EXISTS transactions (
    lane VARCHAR,
    id UUID,
    event INT,
    ts TIMESTAMP,
    PRIMARY KEY (lane, id, event)
);

ALTER TABLE transactions SPLIT AT VALUES ('ACH'), ('WIRE'), ('DEPO');
