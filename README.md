# AWS-Glue-PySpark-Incrementality-and-Parallelism

Solve Incrementality and Parallelism using Custom PySpark code in AWS Glue
In order to run this demo, please feel free to download and setup this custom Cloudformation template to quickly spin up the required s3 and Redshift resources
https://github.com/alexriley7/AWS-Glue-PySpark-Incrementality-and-Parallelism/blob/main/redshift-s3-cloudformation/redshift-s3-cloudformation.template

AWS Glue is a serverless data integration service that makes it easy for analytics users to discover, prepare, move, and integrate data from multiple sources.

Commonly uses are jobs reading stage, performs data enrichment, does validation, and then use this not-well curated data to store into two different targets for reporting.

While AWS Glue consolidates major data integration capabilities into a single service, some issues might happen when of the shelf data connectors are not prepared to deal with messy transactional data in incremental loads.

This project we will discuss the use of Spark multithreading and Job Bookmarks in order to solve some common issues that might happen during the execution.






<img src="https://github.com/alexriley7/AWS-Glue-PySpark-Incrementality-and-Parallelism/blob/main/readme/Dancing-Monkey.gif" />
