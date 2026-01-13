#!/bin/bash

source .env

sqlx database create
cargo sqlx prepare
