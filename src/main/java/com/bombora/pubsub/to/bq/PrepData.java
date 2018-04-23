package com.bombora.pubsub.to.bq;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.joda.time.DateTime;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;
import com.google.gson.Gson;

public  class PrepData {
	private final static Logger LOG = LoggerFactory.getLogger(PrepData.class);

	public static class ToTableRow extends DoFn<String, TableRow> {
		private final static Logger LOG = LoggerFactory.getLogger(ToTableRow.class);
		private static final long serialVersionUID = 1L;
		private boolean owTimestamp, debugMode;
		private DateFormat sourceDateFormat;

		public ToTableRow(boolean owTimestamp, boolean debugMode) {
			this.owTimestamp = owTimestamp;
			this.debugMode = debugMode;
			sourceDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			sourceDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
		}
		@ProcessElement
		public void processElement(ProcessContext c) {
			try{
				String json = (String)c.element();
				if(debugMode)
					LOG.info(json);
				Gson gson = new Gson();	
				
				if(isArray(json)){
					TableRow [] outputRows = gson.fromJson(json, TableRow[].class);
					if(outputRows != null && outputRows.length>0){
						for(TableRow outputRow : outputRows){
							if (owTimestamp && outputRow.containsKey("timestamp")) {			
								LOG.debug("Timestamp overwrite to - " + sourceDateFormat.format(new Date()));
								outputRow.set("timestamp", sourceDateFormat.format(new Date()));

							}
							c.output(outputRow);
						}
					}
				}
				else{

					TableRow outputRow = gson.fromJson(json, TableRow.class);
					if (owTimestamp && outputRow.containsKey("timestamp")) {			
						LOG.debug("Timestamp overwrite to - " + sourceDateFormat.format(new Date()));
						outputRow.set("timestamp", sourceDateFormat.format(new Date()));
					}
					c.output(outputRow);

				}
			}
			catch(Exception e){       
				LOG.error(ExceptionUtils.getStackTrace(e));
			}
		}

		public boolean isArray(String Json) {
			try {
				new JSONObject(Json);
				return false;
			} 
			catch (JSONException ex) {
				try {
					new JSONArray(Json);
					return true;
				}
				catch (JSONException ex1) {
					throw new JSONException(ex1);
				}
			}
		}
	}	
}



