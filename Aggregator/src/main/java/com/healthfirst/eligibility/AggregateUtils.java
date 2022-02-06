package com.healthfirst.eligibility;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.google.gson.JsonObject;

public class AggregateUtils {

	private DynamoDB dynamoDb;
	private String DYNAMO_DB_TABLE_NAME = "ods-ddb-eligibility";
	private Regions REGION = Regions.US_EAST_1;

	private static final Logger logger = LogManager.getLogger(AggregateUtils.class);
	private String PK = "hf_member_num_cd";

	DynamoDBUtils dbUtils = null;

	public void initDynamoDbClient() {

		dbUtils = new DynamoDBUtils();
	}

	public String aggregate(JsonObject payload) {
		DynamoDBUtils dbUtils = new DynamoDBUtils();
		Item it = null;
		it = dbUtils.getItem(payload.get(PK).getAsString());
		if (it == null)
			it = new Item(); // This is to start new item.

		List<Item> itL = new ArrayList<Item>();
		itL.add(it);
		payload.keySet().stream().forEach(key -> {
			Item iit = itL.get(0);
			logger.info(key);
			iit = iit.withString(key, payload.get(key).getAsString());
			itL.set(0, iit);
		});
		dbUtils.updateItem(itL.get(0));
		
		return itL.get(0).toJSON();
	}

}
