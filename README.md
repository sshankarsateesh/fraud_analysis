# Fraud Analytics

## Overview

Pyspark program to report fraud transactions by various dimensions.

## Prequisites
Postgress DB instance
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
1. Create Docker Network
    ## docker network create mynet1 
1. setup up standalone postrgress with docker
    ## docker run -d --net mynet1 --name my_postgres -v my_dbdata:/var/lib/postgresql/data -e POSTGRES_PASSWORD=pass123$ postgres:11
2. get into the above container
    ## docker exec -it  my_postgres /bin/bash
3. get into psql and create database 'creditdb
    ## psql -U postgres
    ## CREATE DATABASE creditdb;


## Docker Build
1. From app directory after cloning
   ## docker build -t fraud_anlytcs -f build/Dockerfile .

## Docker Run
   ## docker run  -e FRAUD='fraud.zip' -e TRANS1='transaction-001.zip' -e TRANS2='transaction-002.zip'  -e POSTUSER='postgres' -e  POSTPWD='pass123$' -p 80:80 -v  $(pwd):/tmp --net mynet1 fraud_anlytcs

## Logs
   1. Logs can be found at /src/logs

## Output
   1. Fraud Transactions by Vendor - Stored as JSON file in the app directory
   2. Fraud Transactions by State - Stored as JSON file in app directory
   3. Dataset with Masked Data - Stored as Parquet Format with Snappy in the app directory
