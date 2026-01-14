# Harvester

OAI-PMH record harvester.

The harvester requires a postgres connection. The default connection config is in `.env` but can be overriden by `.env.local`. To setup:

```bash
cargo install --version="~0.8" sqlx-cli \
    --no-default-features \
    --features rustls,postgres

./scripts/init_db.sh
```

This will create the database and regenerate sqlx query files if needed.
The latter should be committed to version control.

## Running locally

Using cargo for harvesting:

```bash
cargo run -- harvest -m oai_ead -r fixtures/rules.txt https://test.archivesspace.org/oai
```

Using cargo for indexing (ArcLight):

```bash
cargo run -- index arclight \
    allen-doe-research-center \
    "https://test.archivesspace.org/oai" \
    "Allen Doe Research Center"
```

### Rules for metadata extraction

This is an optional feature (though required for indexing). Omit the `-r` arg to bypass.

A rules file looks like:

```txt
title,unittitle,required
unit_id,unitid,required
repository,repository/corpname,required
```

- col 1 is used as a json attribute key for grouping values
- col 2 identifies a path in the oai xml to scan for values
- col 3 can be empty or "required", with the latter enforcing an error if a value is not found
