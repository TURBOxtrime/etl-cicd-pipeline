# BigMart ETL CI/CD Pipeline (AWS Glue & GitHub Actions)

## Overview
Automated ETL pipeline for processing BigMart sales data using PySpark on AWS Glue. Fully CI/CD enabled via GitHub Actions.

## Features
- Upload ETL scripts automatically to S3
- Update Glue job script automatically
- Trigger Glue job & crawler via CI/CD
- Automate Data Catalog updates

## Tech Stack
- Python, PySpark
- AWS Glue, S3, Crawler
- GitHub Actions CI/CD
- Parquet storage for analytics

## How to Run
1. Fork repo & configure GitHub Secrets:
   - AWS_ACCESS_KEY_ID
   - AWS_SECRET_ACCESS_KEY
   - AWS_REGION
   - SOURCE_BUCKET
   - GLUE_JOB_NAME
   - GLUE_CRAWLER_NAME
2. Push to `main` branch → pipeline triggers automatically
3. Check Glue job & crawler status in AWS console

