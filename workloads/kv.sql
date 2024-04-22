-- choose desired table name and column types
CREATE TABLE <table_name> (
    k <col_type> NOT NULL,
    v <col_type> NOT NULL,
    ts TIMESTAMPTZ NOT NULL DEFAULT now():::TIMESTAMPTZ ON UPDATE now():::TIMESTAMPTZ, -- optional but nice to have
    CONSTRAINT pk PRIMARY KEY (k ASC)
);
