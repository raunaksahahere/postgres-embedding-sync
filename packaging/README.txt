embedsync @VERSION@ (@VARIANT@)
=================================

embedsync keeps a pgvector search index in sync with one PostgreSQL table.
Everything it needs is in this installation, including the embedding service:
@MODELS@

Get started (in a terminal):

  embedsync setup        guided: connects to your PostgreSQL, picks the table,
                         columns and model, creates roles, writes the config
  embedsync run          start syncing (keep it running)
  embedsync search "..." search, from another terminal
  embedsync doctor       check that everything works
  embedsync help         all commands

You need PostgreSQL 16, 17 or 18 with the pgvector extension. If you do not
have it, `embedsync setup` tells you exactly how to install it on this system.

Setup configures one table. To sync several tables or use composite keys,
edit the config as in examples/embedsync-multi.yaml (in the repository).

Upgrading? Stop 'embedsync run', then run 'embedsync init --admin-url <admin URL>'
before starting it again.

Documentation: https://github.com/raunaksahahere/postgres-embedding-sync
License: Apache-2.0 (see LICENSE and NOTICE).
