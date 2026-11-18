ALTER TABLE nodes
ADD COLUMN certificate_identity VARCHAR(255);

CREATE UNIQUE INDEX idx_nodes_certificate_identity_unique
ON nodes(certificate_identity)
WHERE certificate_identity IS NOT NULL;
