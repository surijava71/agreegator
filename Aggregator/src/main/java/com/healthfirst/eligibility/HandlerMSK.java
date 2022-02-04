package com.healthfirst.eligibility;

import java.util.Base64;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KafkaEvent;
import com.amazonaws.services.lambda.runtime.events.KafkaEvent.KafkaEventRecord;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

public class HandlerMSK implements RequestHandler<KafkaEvent, String> {
	private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();
	private static final Logger logger = LogManager.getLogger(HandlerMSK.class);
	SimpleProducer prod = null;
	DynamoDBUtils dbUtils = null;

	public HandlerMSK() {
		dbUtils = new DynamoDBUtils();
		dbUtils.initDynamoDbClient();
		prod = new SimpleProducer();
	}

	@Override
	public String handleRequest(KafkaEvent kafkaEvent, Context context) {
		String response = "200 OK";

		logger.info("Aggrerator  EVENT: {} \n", gson.toJson(kafkaEvent));

		java.util.List<KafkaEventRecord> l = null;

		String key = null;
		if (kafkaEvent.getRecords().keySet() != null)
			key = kafkaEvent.getRecords().keySet().stream().findFirst().get();
		if (key == null) return "400 Somthing wrong";
		
		logger.info("Source Topic :: " + key );
		l = kafkaEvent.getRecords().get(key);
		for (KafkaEventRecord r : l) {
			byte[] decodedBytes = Base64.getDecoder().decode(r.getValue());
			String decodedString = new String(decodedBytes);
			logger.info("Message : {} \n", decodedString);
			//Employee employee = new Employee();
			JsonObject payload = null;
			try {

				payload = new Gson().fromJson(decodedString, JsonObject.class);
				//String custid = payload.get("hf_customer_master_id_cd");
				//String memid = payload.get("hf_member_num_cd");
				String custid = payload.get("hf_customer_master_id_cd").getAsString();
				String memid = payload.get("hf_member_num_cd").getAsString();
				String processame = payload.get("KAFKA_PROCESS_NAME").getAsString();
				String custEligibility_span_fk = payload.get("customer_eligibility_span_pk").getAsString();
				String triggerTimeStamp = payload.get("trigger_timestamp").getAsString();
				String providerPK = "providerTestPK";
				payload.addProperty("hf_customer_master_id_cd#hf_member_num_cd", custid + "#" + memid );
				payload.addProperty("KAFKA_PROCESS_NAME#customer_eligibility_span_fk#trigger_timestamp#provider_pk", processame + "#" + custEligibility_span_fk + "#" + triggerTimeStamp + "#" + providerPK);
				
				//hf_customer_master_id_cd#hf_member_num_cd
				logger.info("Payload : {} \n", payload);

			} catch (Exception e) {
				logger.error(e.getMessage());
				return("400 Message formate Error");
			}
			
				
			

			try {
				dbUtils.persistData(payload);

			} catch (Exception e) {
				e.printStackTrace();
				logger.error(e.getMessage());
			}
			try {
				prod.publish("ods-customer-eligibility", decodedString);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			// logger.info("EVENT TYPE: {} \n", kafkaEvent.getClass().toString());
		}

		return response;
	}
}
