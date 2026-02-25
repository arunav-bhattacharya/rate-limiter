-- Increase Oracle connection and cursor limits for load testing.
-- Default PROCESSES=150 is insufficient when Agroal pool max-size=150
-- plus ~40-50 Oracle background processes.
-- Requires DB restart (SPFILE scope); the container startup handles this.

ALTER SYSTEM SET processes=300 SCOPE=SPFILE;
ALTER SYSTEM SET sessions=335 SCOPE=SPFILE;
ALTER SYSTEM SET open_cursors=500 SCOPE=SPFILE;
