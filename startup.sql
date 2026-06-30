-- Boot script: runs on every start, for both new and existing databases.
-- Installs the Quack extension and starts the server. scrooge reads QUACK_TOKEN from
-- the environment and injects it as the `quack_token` variable before running this
-- script (the embedded DuckDB library has no getenv function).
INSTALL quack;
LOAD quack;
CALL quack_identify(
	name => 'scrooge'
);
CALL quack_serve('quack:0.0.0.0:9494', allow_other_hostname => true, token = getvariable('quack_token'));
