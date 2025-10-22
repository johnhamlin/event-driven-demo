-- Source of truth for all domain entities

-- Customers table
CREATE TABLE IF NOT EXISTS customers
(
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    org_id     VARCHAR(255) NOT NULL,
    name       VARCHAR(255) NOT NULL,
    email      VARCHAR(255),
    phone      VARCHAR(50),
    created_at TIMESTAMP        DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP        DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_customers_org_id ON customers (org_id);

-- Work Orders table (source of truth)
CREATE TABLE IF NOT EXISTS work_orders
(
    id           UUID PRIMARY KEY      DEFAULT gen_random_uuid(),
    org_id       VARCHAR(255) NOT NULL,
    customer_id  UUID REFERENCES customers (id),
    status       VARCHAR(50)  NOT NULL DEFAULT 'OPEN',
    title        VARCHAR(500) NOT NULL,
    description  TEXT,
    address      TEXT,
    scheduled_at TIMESTAMP,
    created_at   TIMESTAMP             DEFAULT CURRENT_TIMESTAMP,
    updated_at   TIMESTAMP             DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_work_orders_org_id ON work_orders (org_id);
CREATE INDEX idx_work_orders_status ON work_orders (status);

-- Outbox table (transactional outbox pattern)
-- This is the key to reliable event publishing!
CREATE TABLE IF NOT EXISTS outbox
(
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type     VARCHAR(100) NOT NULL,
    aggregate_id   UUID         NOT NULL,
    aggregate_type VARCHAR(100) NOT NULL,
    payload        JSONB        NOT NULL,
    occurred_at    TIMESTAMP        DEFAULT CURRENT_TIMESTAMP,
    published_at   TIMESTAMP,
    version        INTEGER          DEFAULT 1
);

-- Index for publisher to find unpublished events quickly
CREATE INDEX idx_outbox_unpublished ON outbox (occurred_at)
    WHERE published_at IS NULL;

-- Add a comment explaining the pattern
COMMENT ON TABLE outbox IS
    'Transactional outbox pattern: Events are written atomically with domain data,
     then published asynchronously to ensure reliable event delivery';

-- Helper function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
    RETURNS TRIGGER AS
$$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Triggers to auto-update updated_at
CREATE TRIGGER update_customers_updated_at
    BEFORE UPDATE
    ON customers
    FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_work_orders_updated_at
    BEFORE UPDATE
    ON work_orders
    FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

-- Insert some seed data for testing
INSERT INTO customers (org_id, name, email, phone)
VALUES ('plumbingCo123', 'John Smith', 'john@example.com', '555-0100'),
       ('plumbingCo123', 'Jane Doe', 'jane@example.com', '555-0101')
ON CONFLICT DO NOTHING;
