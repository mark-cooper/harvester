#!/bin/bash

source .env

sqlx database create
sqlx database setup
