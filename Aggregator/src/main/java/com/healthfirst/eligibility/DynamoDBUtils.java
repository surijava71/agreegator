package com.healthfirst.eligibility;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.google.gson.JsonObject;

public class DynamoDBUtils {

	private DynamoDB dynamoDb;
	private String DYNAMO_DB_TABLE_NAME = "ods-ddb-eligibility";
	private Regions REGION = Regions.US_EAST_1;

	private static final Logger logger = LogManager.getLogger(DynamoDBUtils.class);

	public void initDynamoDbClient() {

		AmazonDynamoDBClient client = new AmazonDynamoDBClient();
		logger.info("Init Dynamo DB");
		client.setRegion(Region.getRegion(REGION));
		this.dynamoDb = new DynamoDB(client);
	}

	public PutItemOutcome persistData(JsonObject payload) {
		logger.info("Persist Dynamo DB1");
		Table table = dynamoDb.getTable(DYNAMO_DB_TABLE_NAME);
		logger.info(table.getTableName());
		logger.info("Persist Dynamo DB2");

		List<Item> itL = new ArrayList<Item>();
        itL.add(new Item());
		payload.keySet().stream().forEach(key -> {
			Item it = null;
			if (itL.get(0) == null)
				it = new Item();
			else
				it = itL.get(0);

			logger.info(key);
			it = it.withString(key, payload.get(key).getAsString());
			itL.set(0, it);
		});
		
		logger.info("########" + itL.get(0).getString("hf_member_num_cd"));
	
		
		PutItemOutcome outcome = table.putItem(new PutItemSpec().withItem(itL.get(0)));

		logger.info("after Persist Dynamo DB");
		return outcome;
	}

}