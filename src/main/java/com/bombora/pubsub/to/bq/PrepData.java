package com.bombora.pubsub.to.bq;


import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.gson.Gson;

public  class PrepData {
	private final static Logger LOG = LoggerFactory.getLogger(PrepData.class);

	public static class ToTableRow extends DoFn<String, TableRow> {
		private final static Logger LOG = LoggerFactory.getLogger(ToTableRow.class);
		private static final long serialVersionUID = 1L;

		@Override
		public void processElement(ProcessContext c) {
			try{
				String json = (String)c.element();
				Gson gson = new Gson();	
				TableRow outputRow = gson.fromJson(json, TableRow.class);
				c.output(outputRow);
			}
			catch(Exception e){       
				LOG.error(ExceptionUtils.getStackTrace(e));
			}
		}
	}
}



