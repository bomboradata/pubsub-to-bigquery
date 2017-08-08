# PubSubToBigQuery
A highly configurable Google Cloud Dataflow pipeline that writes data into a Google Big Query table from Pub/Sub

Compile this Dataflow pipeline into a runnable Jar (pubsub-to-bq.jar). Then call the jar with following parameters:

java.exe -jar "C:\Jars\pubsub-to-bq.jar" --runner=BlockingDataflowPipelineRunner --params="<params><workingBucket>gs://your_bucker</workingBucket><maxNumWorkers>1</maxNumWorkers><diskSizeGb>250</diskSizeGb><machineType>n1-standard-1</machineType><keyFile>C:\KeyFiles\YourFile.p12</keyFile><accountEmail>your_account@developer.gserviceaccount.com</accountEmail><projectId>your_project_id</projectId><pipelineName>your_pipeline_name</pipelineName><pubSubTopic>your_pub_topic</pubSubTopic><bqDataSet>your_destination_BQ_dataset</bqDataSet><bqTable>your_destination_BQ_table</bqTable><streaming>true</streaming><zone>us-west1-a</zone><schema>{"fields":[{"description":null,"fields":null,"mode":"REQUIRED","name":"Student_Name","type":"STRING","ETag":null}],"ETag":null}</schema></params>"

Parameters formatted view:http://i.imgur.com/UIapKTT.png
