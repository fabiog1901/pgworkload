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

-- Bank schema
CREATE TABLE IF NOT EXISTS bank (
    id UUID,
    event INT,
    lane STRING,
    ts TIMESTAMP,
    PRIMARY KEY (id, event)
);

-- Balance schema
CREATE TABLE IF NOT EXISTS balance (
    id UUID NOT NULL DEFAULT gen_random_uuid(),
    corr STRING(4) NOT NULL,
    system_dt TIMESTAMP NOT NULL,
    office STRING(4) NOT NULL,
    acct_no STRING(12) NOT NULL,
    sub_acct_no STRING(12) NULL,
    acct_type STRING(2) NOT NULL,
    symbol STRING(25) NOT NULL,
    sym_no INT NOT NULL DEFAULT 0,
    price DECIMAL(14, 6) NOT NULL,
    topen DECIMAL(16, 6) NULL,
    tclose DECIMAL(19, 8) NOT NULL,
    tmktval DECIMAL(19, 6) NULL,
    sopen DECIMAL(16, 6) NULL,
    sclose DECIMAL(19, 8) NOT NULL,
    smktval DECIMAL(19, 6) NULL,
    seg_orig DECIMAL(19, 8) NULL,
    seg_qty DECIMAL(19, 8) NULL,
    seg_fluid DECIMAL(16, 6) NULL,
    memo_rights DECIMAL(16, 6) NULL,
    memo_tender DECIMAL(16, 6) NULL,
    memo_splits DECIMAL(16, 6) NULL,
    memo_merger DECIMAL(16, 6) NULL,
    memo_acats DECIMAL(16, 6) NULL,
    memo_transfer DECIMAL(16, 6) NULL,
    memo_safekeep DECIMAL(16, 6) NULL,
    ex_req_value DECIMAL(19, 6) NULL,
    ho_req_value DECIMAL(19, 6) NULL,
    ex_req_method STRING(10) NULL,
    exec_symbol STRING(25) NULL,
    g_tcost DECIMAL(19, 6) NULL,
    n_tcost DECIMAL(19, 6) NULL,
    memo_firmuse DECIMAL(16, 6) NULL,
    fed_req_value DECIMAL(19, 6) NULL,
    hold_type STRING NOT NULL DEFAULT 'L',
    seg_earlyrel DECIMAL(19, 8) NULL,
    factor DECIMAL(19, 12) NULL,
    factor_dt DATE NULL,
    CONSTRAINT "primary" PRIMARY KEY (id ASC),
    UNIQUE INDEX balance_i2 (
        acct_no ASC,
        system_dt ASC,
        corr ASC,
        office ASC,
        acct_type ASC,
        sym_no ASC,
        hold_type ASC
    ),
    INDEX balance_i3 (sym_no ASC, system_dt ASC)
);
