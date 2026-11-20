ALTER TABLE services
ADD COLUMN deployment_failure_reason VARCHAR(64) NOT NULL DEFAULT '';
