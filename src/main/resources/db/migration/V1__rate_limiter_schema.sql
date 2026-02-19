-- Rate limit configuration (dynamic, versioned)
CREATE TABLE rate_limit_config (
    id               NUMBER(19) GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    config_name      VARCHAR2(128)  NOT NULL,
    max_per_window   NUMBER(10)     NOT NULL,
    window_size_secs NUMBER(5)      NOT NULL,
    effective_from   TIMESTAMP      NOT NULL,
    is_active        NUMBER(1)      DEFAULT 1 NOT NULL,
    created_at       TIMESTAMP      DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT chk_max_per_window CHECK (max_per_window > 0),
    CONSTRAINT chk_window_size    CHECK (window_size_secs > 0)
);

CREATE INDEX idx_config_name_active ON rate_limit_config (config_name, is_active);

-- Per-window slot counter (config-agnostic concurrency control table)
CREATE TABLE rate_limit_window_counter (
    window_start TIMESTAMP NOT NULL,
    slot_count   NUMBER(10) DEFAULT 0 NOT NULL,
    CONSTRAINT pk_window_counter PRIMARY KEY (window_start)
);

-- Immutable slot assignment record
CREATE TABLE scheduled_event_slot (
    id             NUMBER(19) GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id       VARCHAR2(256)  NOT NULL,
    requested_time TIMESTAMP      NOT NULL,
    window_start   TIMESTAMP      NOT NULL,
    slot_index     NUMBER(10)     NOT NULL,
    scheduled_time TIMESTAMP      NOT NULL,
    config_id      NUMBER(19)     NOT NULL,
    created_at     TIMESTAMP      DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT uq_event_id UNIQUE (event_id),
    CONSTRAINT fk_slot_config FOREIGN KEY (config_id) REFERENCES rate_limit_config(id)
);

CREATE INDEX idx_slot_window  ON scheduled_event_slot (window_start);
CREATE INDEX idx_slot_created ON scheduled_event_slot (created_at);
