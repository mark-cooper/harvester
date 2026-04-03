# Harvester

OAI-PMH record harvester.

## Setup

[Mise](https://mise.jdx.dev/) is recommended for version management.

```bash
mise trust
mise install
mise run gems
```

The harvester requires a postgres connection. The default connection config is in `.env` but can be overriden by `.env.local`. To setup:

```bash
docker compose up -d postgres postgrest
```

This initializes:
- `harvester` database owned by `admin`
- `harvester_reader` role for PostgREST with read access on `public` tables (including future tables)

## Running locally

Using cargo for harvesting:

```bash
cargo run -- harvest -m oai_ead -r fixtures/rules.txt https://test.archivesspace.org/oai
```

Using cargo for indexing (ArcLight):

```bash
cargo run -- index arclight run \
    allen-doe-research-center \
    "https://test.archivesspace.org/oai" \
    "Allen Doe Research Center"
```

This uses a range of default values so will only work if your setup is aligned.
For all options run: `cargo run -- index arclight --help`.

Retry failed index operations for a specific endpoint/repository pair:

```bash
cargo run -- index arclight \
    allen-doe-research-center \
    "https://test.archivesspace.org/oai" \
    "Allen Doe Research Center" \
    --retry \
    --message-filter "timed out" \
    --max-attempts 5
```

Requeue all parsed/deleted records for a specific endpoint/repository pair:

```bash
cargo run -- index arclight \
    "https://test.archivesspace.org/oai" \
    "Allen Doe Research Center" \
    --reindex
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

## DB reset

```bash
# adjust envvar values as appropriate
PGHOST=localhost PGUSER=admin PGPASSWORD=admin psql \
    -c "DROP DATABASE harvester;"

./scripts/init_db.sh
```

Resetting index failed records to pending via the db:

```
UPDATE oai_records
SET index_status = 'pending', index_attempts = 0, index_message = ''
WHERE index_status = 'index_failed'
AND endpoint = 'https://example.com/oai';
```

## Docker

```bash
# start postgres + postgrest
docker compose up -d postgres postgrest

# build harvester image via compose
docker compose build harvester

# run harvest (uses defaults from .env)
docker compose run --rm harvester harvest https://test.archivesspace.org/oai

# run index (override SOLR_URL as needed)
docker compose run --rm \
    -e SOLR_URL=http://host.docker.internal:8983/solr/arclight \
    harvester index arclight \
    "allen-doe-research-center" \
    "https://test.archivesspace.org/oai" \
    "Allen Doe Research Center"
```

Override any default with `-e KEY=value` on `docker compose run`.

For rootless Docker, if bind mount permissions fail, add `--user root` to `docker compose run` commands.

## PostgREST

- [http://localhost:3000/oai_records](http://localhost:3000/oai_records)

If you get an error like:

```json
{"code":"PGRST205","details":null,"hint":null,"message":"Could not find the table 'public.oai_records' in the schema cache"}
```

Run `docker compose restart postgrest`.
