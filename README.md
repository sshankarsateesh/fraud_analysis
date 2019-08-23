# Fraud Analytics

## Overview

Pyspark program to report fraud transactions by various dimensions.

## Prequisites
Postgress DB=
Docker=2.1.0.1

Directory Structure
pci/src
pci/build

Files
src/fraud_analytics.py -> Source Code
src/*zip files -> Test Data
src/log4j.properties -> Spark Logging Configuration
build/Dockerfile -> Docker configuration file


## Setup

Postgress
setup up standalone postrgress with docker
-> docker run -d --name my_postgres -e POSTGRES_PASSWORD=pass123$ -v my_dbdata:/var/lib/postgresql/data -p 54320:5432 postgres:11
get into the above container
-> docker exec -it  my_postgres /bin/bash
get into psql and create database 'creditdb
psql -U postgres
CREATE DATABASE creditdb;


## Docker Build
From app directory after cloning
docker build -t fraud_anlytcs -f build/Dockerfile .

## Docker Run
docker run  -e FRAUD='fraud.zip' -e TRANS1='transaction-001.zip' -e TRANS2='transaction-002.zip'  -e POSTUSER='postgres' -e POSTPWD='pass123$' -p 80:80 -v  $(pwd):/tmp --net mynet1 fraud_anlytcs

## Logs
Logs can be found at /src/logs

## Output

Fraud Transactions by Vendor - Stored as JSON file in the app directory
Fraud Transactions by State - Stored as JSON file in app directory
Dataset with Masked Data - Stored as Parquet Format with Snappy in the app directory
