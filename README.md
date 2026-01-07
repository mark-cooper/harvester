# Harvester

OAI-PMH record harvester.

The harvester requires a postgres connection. The default connection config is in `.env` but can be overriden by `.env.local`. To setup:

```bash
cargo install --version="~0.8" sqlx-cli \
    --no-default-features \
    --features rustls,postgres

./scripts/init_db.sh
```

This will create the database and table.

## Running locally

Using cargo:

```bash
cargo run -- harvest -m oai_ead -r ~/rules.txt https://test.archivesspace.org/oai
```

### Rules for metadata extraction

This is an optional feature. Omit the `-r` arg to bypass.

A rules file looks like:

```txt
title,unittitle,required
unit_id,unitid,required
repository,repository/corpname,required
```

- col 1 is used as a json attribute key for grouping values
- col 2 identifies a path in the oai xml to scan for values
- col 3 can be empty or "required", with the latter enforcing an error if a value is not found
