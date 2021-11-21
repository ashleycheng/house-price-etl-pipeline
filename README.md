# house-price-elt-pipeline
This project contains the process from building a web crawler to extract the raw data of house price to create ETL pipelines using Google Could Platform services. 

### Basic flow of the ETL pipeline
![](https://i.imgur.com/tXjUX1w.png)
The ETL pipelines are built with both Apache Beam using Cloud Dataflow and Spark using Cloud Dataproc for loading real estate transactions data into BigQuery, and the data can be visualized in Data Studio. The project also uses Cloud Function to monitor if a new file is uploaded in the GCS bucket and trigger the pipeline automatically.

## 1. Get Started
### The house price data
[Actual price registration of real estate transactions data](https://plvr.land.moi.gov.tw/DownloadOpenData) in Taiwan has been released since 2012, which refers to the transaction information includes: position and area of real estate, total price of land and building, parking space related information, etc. We can use the data to observe the changes in house prices over time or predict the house price trend in various regions.

### Setup and requirements
Set up on Google Cloud Platform: 
* [Set up a project](https://cloud.google.com/dataproc/docs/guides/setup-project?hl=zh-tw)
* [Create a Cloud Storage bucket](https://cloud.google.com/storage/docs/creating-buckets)
* [Create a Dataproc cluster](https://cloud.google.com/dataproc/docs/guides/create-cluster?hl=zh-tw)
* [Create a Bigquery dataset](https://cloud.google.com/bigquery/docs/datasets#console)

Project is created with:
* Python version: 3.7
* Apache beam version: 2.33.0
* Pyspark version: 3.2.0


## 2. Use a web crawler to download the historical data
Run the web crawler to download historical actual price data in csv format, and upload the files to the Google Cloud Storage bucket.



First, set up the local Python development environment and install packages from [requirements.txt](https://github.com/ashleycheng/house-price-elt-pipeline/blob/main/web_crawler/requirements.txt):
```
$ pip install -r requirements.txt
```
Open [crawler.py](https://github.com/ashleycheng/house-price-elt-pipeline/blob/main/web_crawler/crawler.py) file, replace `YOUR_DIR_PATH` with a local directory to store download data, replace `projectID` with your Google Cloud project ID, and replace `GCS_BUCKET_NAME` with the name of your Cloud Storage bucket. Then run the web crawler:
```
$ python crawler.py
```

## 3. Build ETL pipelines on GCP
There are two versions of ETL pipelines that read source files from Cloud Storage, apply some transformations and load the data into BigQuery. One of the ETL pipelines based on Apache beam uses Dataflow to process the data for analytics of land transaction. The other ETL pipeline based on Apache Spark uses Dataproc to proccess the data for analytics of building transaction.


Let’s start by opening a session in Google Cloud Shell. Run the following commands to set the project property with your project ID.
```
$ gcloud config set project [projectID]
```

### Run the pipeline using Dataflow for land data
The file [etl_pipeline_beam.py](https://github.com/ashleycheng/house-price-elt-pipeline/blob/main/etl_pipeline_beam.py) contains the Python code for the etl pipeline with Apache beam. We can upload the file using the Cloud Shell Editor.


Run actual_price_etl.py to create a Dataflow job which runs the DataflowRunner. Notice that we need to set the Cloud Storage location of the staging and template file, and set the region in which the created job should run.
```
$ python etl_pipeline_beam.py \
--project=projectID \
--region=region \
--runner=DataflowRunner \
--staging_location=gs://BUCKET_NAME/staging \
--temp_location=gs://BUCKET_NAME/temp \
--save_main_session
```

### Run the pipeline using Dataproc for building data
The file [etl_pipeline_spark.py](https://github.com/ashleycheng/house-price-elt-pipeline/blob/main/etl_pipeline_spark.py) contains the Python code for the etl pipeline with Apache Spark. We can upload the file using the Cloud Shell Editor.

Submit [etl_pipeline_spark.py](https://github.com/ashleycheng/house-price-elt-pipeline/blob/main/etl_pipeline_spark.py) to your Dataproc cluster to run the Spark job. We need to set the cluster name, and set the region in which the created job should run. To write data to Bigquery, the jar file of [spark-bigquery-connector](https://github.com/GoogleCloudDataproc/spark-bigquery-connector) must be available at runtime.
```
$ gcloud dataproc jobs submit pyspark etl_pipeline_spark.py \
--cluster=cluster-name \
--region=region \
--jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar
```
## 4. Use a Cloud Function to trigger Cloud Dataflow
Use the Cloud Fucntion to automatically trigger the Dataflow pipeline when a new file arrives in the GCS bucket.

First, we need to create a Dataflow template for runnig the data pipeline with REST API request called by the Cloud Function. The file [etl_pipeline_beam_auto.py](https://github.com/ashleycheng/house-price-elt-pipeline/blob/main/etl_pipeline_beam_auto.py) contains the Python code for the etl pipeline with Apache beam. We can upload the file using the Cloud Shell Editor. 

#### Create a Dataflow template
Use [etl_pipeline_beam_auto.py](https://github.com/ashleycheng/house-price-elt-pipeline/blob/main/etl_pipeline_beam_auto.py) to create a Dataflow template. Note that we need to set the Cloud Storage location of the staging, temporary and template file, and set the region in which the created job should run.

```
python -m etl_pipeline_beam_auto \
    --runner DataflowRunner \
    --project projectID \
    --region=region \
    --staging_location gs://BUCKET_NAME/staging \
    --temp_location gs://BUCKET_NAME/temp \
    --template_location gs://BUCKET_NAME/template \
    --save_main_session
```

#### Create a Cloud Function
Go to the Cloud Function GUI and manually create a function, set `Trigger` as `Cloud Storage`, `Event Type` as `Finalize/Create` , and choose the GCS bucket which needs to be monitored. Next, write the function itself, use the code in [main.py](https://github.com/ashleycheng/house-price-elt-pipeline/blob/main/cloud_function/main.py) file. Note that the user defined parameter `input` is passed to the Dataflow pipeline job. Finally, click on depoly and now your function is ready to execute and start the Dataflow pipeline when a file is uploaded in your bucket.

## Results
When each ETL pipeline is completed and succeeded, navigating to BigQuery to verify that the data is successfully loaded in the table.


![](https://i.imgur.com/DRR8MRP.png)
*BigQuery - land_data table*


Now the data is ready for analytics and reporting. Here, we calculate average price by year in BigQuery, and visualize the results in Data Studio.

![](https://i.imgur.com/tUXMVGI.png)
*Data Studio - Average land price by year in Yilan County*
