package com.bombora.pubsub.to.bq;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.text.ParseException;
import java.util.Arrays;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang.StringUtils;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;


import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.gson.Gson;


public class PubSubToBQPipeline {

	private static String workingBucket;
	private static String projectId;;
	private static String accountEmail; 
	private static String keyFile;
	private static String zone;
	private static String pipelineName; 
	private static String pubSubTopic;
	private static String pubSubTopicSub;
	private static String bqDataSet;
	private static String bqTable;
	private static String schemaStr;
	private static boolean isStreaming; 
	private static int maxNumWorkers;
	private static int diskSizeGb;
	private static String machineType;
	private static boolean owTimestamp, debugMode;

	public static void main(String[] args) throws GeneralSecurityException, IOException, ParseException, ParserConfigurationException, SAXException {
		String params = null;
		for (int i = 0; i < args.length; i++) {
			if (args[i].startsWith("--params="))
				params = args[i].replaceFirst("--params=", "");
		}

		System.out.println(params);
		init(params);

		GoogleCredentials credentials = ServiceAccountCredentials.fromStream(new FileInputStream(keyFile))
		        .createScoped(Arrays.asList(new String[] { "https://www.googleapis.com/auth/cloud-platform" }));

		DataflowPipelineOptions options = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
		
		options.setRunner(DataflowRunner.class);
		// Your project ID is required in order to run your pipeline on the Google Cloud.
		options.setProject(projectId);
		// Your Google Cloud Storage path is required for staging local files.
		options.setStagingLocation(workingBucket);
		options.setTempLocation(workingBucket + "/temp");
		options.setGcpCredential(credentials);
		options.setServiceAccount(accountEmail);
		options.setMaxNumWorkers(maxNumWorkers);
		options.setDiskSizeGb(diskSizeGb);
		options.setWorkerMachineType(machineType);
		options.setAutoscalingAlgorithm(AutoscalingAlgorithmType.THROUGHPUT_BASED);
		options.setZone(zone);
		options.setStreaming(isStreaming);
		options.setJobName(pipelineName);
		Pipeline pipeline = Pipeline.create(options);
		
		Gson gson = new Gson();
		TableSchema schema = gson.fromJson(schemaStr, TableSchema.class);
		
		PCollection<String> streamData = null;
		if(pubSubTopicSub != null && !StringUtils.isEmpty(pubSubTopicSub)){
			streamData = pipeline.apply("ReadPubSub",PubsubIO.readStrings().fromSubscription(String.format("projects/%1$s/subscriptions/%2$s",projectId,pubSubTopicSub)));
		}
		else if(pubSubTopic != null && !StringUtils.isEmpty(pubSubTopic)){
			streamData = pipeline.apply("ReadPubSub",PubsubIO.readStrings().fromTopic(String.format("projects/%1$s/topics/%2$s",projectId,pubSubTopic)));
		}
		
		PCollection<TableRow> tableRow = streamData.apply("ToTableRow",ParDo.of(new PrepData.ToTableRow(owTimestamp, debugMode)));
		
		
		tableRow.apply("WriteToBQ",
				BigQueryIO.writeTableRows()
				.to(String.format("%1$s.%2$s",bqDataSet, bqTable))
				.withSchema(schema)
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

		System.out.println("Starting pipeline " + pipelineName);
		pipeline.run();
	}

	public static void init(String params) throws ParserConfigurationException,
	SAXException, IOException, DOMException, ParseException {
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = factory.newDocumentBuilder();
		Document document = builder.parse(new ByteArrayInputStream(params.getBytes()));
		NodeList nodeList = document.getDocumentElement().getChildNodes();
		
		debugMode = owTimestamp = false;
		
		for (int i = 0; i < nodeList.getLength(); i++) {
			Node node = nodeList.item(i);
			String name = node.getNodeName();
			switch (name) {
			case "projectId":
				projectId = node.getTextContent();
				break;
			case "accountEmail":
				accountEmail = node.getTextContent();
				break;
			case "keyFile":
				keyFile = node.getTextContent();
				break;
			case "workingBucket":
				workingBucket = node.getTextContent();
				break;
			case "maxNumWorkers":
				maxNumWorkers = Integer.parseInt(node.getTextContent());
				break;
			case "diskSizeGb":
				diskSizeGb = Integer.parseInt(node.getTextContent());
				break;
			case "machineType":
				machineType = node.getTextContent();
				break;
			case "pipelineName":
				pipelineName = node.getTextContent();
				break;
			case "bqDataSet":
				bqDataSet = node.getTextContent();
				break;
			case "bqTable":
				bqTable = node.getTextContent();
				break;
			case "streaming":
				isStreaming = Boolean.parseBoolean(node.getTextContent());
				break;
			case "schema":
				schemaStr = node.getTextContent();
				break;
			case "zone":
				zone = node.getTextContent();
				break;
			case "pubSubTopic":
				pubSubTopic = node.getTextContent();
				break;
			case "pubSubTopicSub":
				pubSubTopicSub = node.getTextContent();
				break;
			case "owTimestamp":
				owTimestamp = Boolean.parseBoolean(node.getTextContent());
			case "debugMode":
				debugMode = Boolean.parseBoolean(node.getTextContent());
        break;
			}
		}
		return;
	}

}
