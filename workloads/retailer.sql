-- Retailer schema
CREATE TABLE IF NOT EXISTS credits (
    id INT2 NOT NULL,
    code UUID NOT NULL,
    channel STRING(1) NOT NULL,
    pid INT4 NOT NULL,
    end_date DATE NOT NULL,
    STATUS STRING(1) NOT NULL,
    start_date DATE NOT NULL,
    CONSTRAINT "primary" PRIMARY KEY (id ASC, code ASC),
    INDEX credits_pid_idx (pid ASC),
    INDEX credits_code_id_idx (code ASC, id ASC) STORING (channel, STATUS, end_date, start_date)
);

CREATE TABLE IF NOT EXISTS offers (
    id INT4 NOT NULL,
    code UUID NOT NULL,
    token UUID NOT NULL,
    start_date DATE,
    end_date DATE,
    CONSTRAINT "primary" PRIMARY KEY (id ASC, code ASC, token ASC),
    INDEX offers_token_idx (token ASC)
);
