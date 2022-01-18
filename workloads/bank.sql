CREATE TABLE IF NOT EXISTS transactions (
    id UUID,
    event INT,
    lane STRING,
    ts TIMESTAMP,
    PRIMARY KEY (id, event)
);

CREATE TABLE IF NOT EXISTS ref_data (
    my_sequence INT PRIMARY KEY,
    my_costant STRING,
    my_uuid UUID,
    my_choice STRING,
    my_integer INT,
    my_timestamp TIMESTAMP,
    my_date DATE,
    my_time TIME,
    -- my_bytes BYTES,
    my_string STRING
);
