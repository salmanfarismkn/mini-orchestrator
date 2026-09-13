ALTER TABLE services
ADD COLUMN autoscaling_scale_up_cooldown_seconds
INTEGER NOT NULL DEFAULT 60;

ALTER TABLE services
ADD COLUMN autoscaling_scale_down_cooldown_seconds
INTEGER NOT NULL DEFAULT 300;

ALTER TABLE services
ADD COLUMN autoscaling_required_observations
INTEGER NOT NULL DEFAULT 3;

ALTER TABLE services
ADD COLUMN autoscaling_max_scale_step
INTEGER NOT NULL DEFAULT 2;

ALTER TABLE services
ADD COLUMN autoscaling_last_scaled_at TIMESTAMP;