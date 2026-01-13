#!/bin/bash

source .env

sqlx database create
sqlx migrate run
cargo sqlx prepare
