ALTER TABLE services
ADD COLUMN status VARCHAR(32) NOT NULL DEFAULT 'ACTIVE';

CREATE INDEX idx_services_status ON services(status);