CREATE TABLE IF NOT EXISTS ref_data (
    my_sequence INT PRIMARY KEY,
    my_costant VARCHAR,
    my_uuid UUID,
    my_choice VARCHAR,
    my_integer INT,
    my_float FLOAT ARRAY,
    my_timestamp TIMESTAMP,
    my_date DATE,
    my_time TIME,
    -- my_bytes BYTES,
    my_string VARCHAR [],
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
