#!/bin/bash

source .env

sqlx database drop
sqlx database create
sqlx database setup
