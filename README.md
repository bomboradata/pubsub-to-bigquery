# PubSubToBigQuery
A highly configurable Google Cloud Dataflow pipeline that writes data into a Google Big Query table from Pub/Sub

Compile this Dataflow pipeline into a runnable Jar (pubsub-to-bq.jar). Then call the jar with following parameters:

```bash
java.exe -jar "C:\Jars\pubsub-to-bq.jar" --runner=BlockingDataflowPipelineRunner --params="<params><workingBucket>gs://your_bucket</workingBucket><maxNumWorkers>1</maxNumWorkers><diskSizeGb>250</diskSizeGb><machineType>n1-standard-1</machineType><keyFile>C:\KeyFiles\YourFile.json</keyFile><accountEmail>your_account@developer.gserviceaccount.com</accountEmail><projectId>your_project_id</projectId><pipelineName>your_pipeline_name</pipelineName><pubSubTopic>your_pub_topic</pubSubTopic><bqDataSet>your_destination_BQ_dataset</bqDataSet><bqTable>your_destination_BQ_table</bqTable><streaming>true</streaming><zone>us-west1-a</zone><schema>{"fields":[{"description":null,"fields":null,"mode":"REQUIRED","name":"Student_Name","type":"STRING","ETag":null}],"ETag":null}</schema></params>"
```
Parameters formatted view:
```xml
<params>
   <workingBucket>gs://your_bucket</workingBucket>
   <maxNumWorkers>1</maxNumWorkers>
   <diskSizeGb>250</diskSizeGb>
   <machineType>n1-standard-1</machineType>
   <keyFile>C:\KeyFiles\YourFile.json</keyFile>
   <accountEmail>your_account@developer.gserviceaccount.com</accountEmail>
   <projectId>your_project_id</projectId>
   <pipelineName>your_pipeline_name</pipelineName>
   <pubSubTopic>your_pub_topic</pubSubTopic>
   <bqDataSet>your_destination_BQ_dataset</bqDataSet>
   <bqTable>your_destination_BQ_table</bqTable>
   <streaming>true</streaming>
   <zone>us-west1-a</zone>
   <schema>{"fields":[{"description":null,"fields":null,"mode":"REQUIRED","name":"Student_Name","type":"STRING","ETag":null}],"ETag":null}</schema>
</params>
```

- Only provide .json key files for GCP.
- Pipeline can support queues with batched messages.
- Set the debug flag setting if you want pipeline to log incoming messages
- This pipeline can also support streaming data into a date partitioned BQ table with a streaming pipeline. [Details](http://meethassan.net/2017/08/07/streaming-writes-into-a-date-partitioned-bigquery-table-using-a-dataflow-streaming-pipeline/)
