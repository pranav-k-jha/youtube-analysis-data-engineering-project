# YouTube Analysis Data Engineering Project

## Overview
This project aims to securely manage, streamline, and analyze structured and semi-structured YouTube video data based on video categories and trending metrics.

## Project Goals
- **Data Ingestion:** Develop a mechanism to ingest data from various sources.
- **ETL System:** Transform raw data into a proper format.
- **Data Lake:** Create a centralized repository to store data from multiple sources.
- **Scalability:** Ensure the system can scale as data volume increases.
- **Cloud:** Utilize AWS to process large volumes of data.
- **Reporting:** Build a dashboard to answer key questions based on the data.

## Services Used
- **Amazon S3:** Object storage service providing scalability, data availability, security, and performance.
- **AWS IAM:** Identity and access management to securely manage access to AWS services and resources.
- **Amazon QuickSight:** Scalable, serverless, machine learning-powered business intelligence (BI) service.
- **AWS Glue:** Serverless data integration service for discovering, preparing, and combining data for analytics, machine learning, and application development.
- **AWS Lambda:** Compute service for running code without managing servers.
- **AWS Athena:** Interactive query service for analyzing data in Amazon S3 using standard SQL.

## Dataset Used
This project uses a Kaggle dataset containing statistics (CSV files) on daily popular YouTube videos over several months. The dataset includes up to 200 trending videos published daily across various regions. Data fields include video title, channel title, publication time, tags, views, likes, dislikes, description, and comment count. Additionally, a `category_id` field is included in the JSON file specific to each region.

[Kaggle YouTube New Dataset Link](https://www.kaggle.com/datasets/datasnaek/youtube-new)

