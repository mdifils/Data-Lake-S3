# Data lake using S3 bucket

## Introduction

A music streaming startup, Sparkify, has grown their user base and song database
even more and want to move their data warehouse to a data lake. Their data resides
in S3, in a directory of JSON logs on user activity on the app, as well as a
directory with JSON metadata on the songs in their app.

My task as data engineer, is to build an ETL pipeline that extracts their data
from S3, processes them using Spark, and loads the data back into S3 as a set of
dimensional tables. This will allow their analytics team to continue finding
insights in what songs their users are listening to.

## AWS EMR cluster

Installing and configuring spark is not easy. That why I'll leverage the power
of amazon ElasticMapReduce. Go to AWS management console and search for EMR
service. Then click on `create cluster` and configure the cluster as follow:

![Launch EMR](images/emr-cluster.png)

When the cluster is running or in a waiting status. Click on `notebook`
(left side) and then click on `create notebook` blue button:

![create notebook](images/create-notebook.png)

Click on `Open in JupyterLab` button when the notebook is ready:

![launch notebook](images/open-notebook.png)
