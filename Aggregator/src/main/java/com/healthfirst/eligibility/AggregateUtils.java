package com.healthfirst.eligibility;

import java.util.HashMap;
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

 
public class AggregateUtils {
  
 private DynamoDB dynamoDb;
 private String DYNAMO_DB_TABLE_NAME = "Employee";
 private Regions REGION = Regions.US_EAST_1;
 
 
 private static final Logger logger = LogManager.getLogger(HandlerMSK.class);
 
 private AmazonDynamoDBClient client;
  public void initDynamoDbClient() {
	
	  
  client = new AmazonDynamoDBClient();
  logger.info("Init Dynamo DB");
  client.setRegion(Region.getRegion(REGION));
  this.dynamoDb = new DynamoDB(client);
 }
  
 public PutItemOutcome aggregateEvent(HashMap<String,Object> event) {
	 logger.info("Persist Dynamo DB1");
	 Table table = dynamoDb.getTable(DYNAMO_DB_TABLE_NAME);
	 
	 
	 Map<String,AttributeValue> attributeValues = new HashMap<>();
	    attributeValues.put("hf_member_num_cd",new AttributeValue().withS((String)event.get("hf_member_num_cd")));
	  /* 
	    UpdateItemRequest updateItemRequest = new UpdateItemRequest()
	        .withTableName("Eligibility")
	        .addKeyEntry("hf_member_num_cd",new AttributeValue().withS((String)event.get("hf_member_num_cd")))
	        .addAttributeUpdatesEntry("first_nm",new AttributeValueUpdate().withValue(new AttributeValue().withS((String)event.get("first_nm")))
	        		.addAttributeUpdatesEntry("first_nm",new AttributeValueUpdate().withValue(new AttributeValue().withS((String)event.get("first_nm")))
	        		        
	            
	        		
	        		);
	        */
	        
	 
	/* 
	 attributeValues.put("email",new AttributeValue().withS(email));
	    attributeValues.put("fullname",new AttributeValue().withS(fullName));

	    UpdateItemRequest updateItemRequest = new UpdateItemRequest()
	        .withTableName("Eligibility");
	        .addKeyEntry("hf_member_num_cd",new AttributeValue().withS(email))
	        .addAttributeUpdatesEntry("fullname",
	            new AttributeValueUpdate().withValue(new AttributeValue().withS(fullName)));

	    UpdateItemResult updateItemResult = client.updateItem(updateItemRequest);
	    
	    
	 
	 logger.info(table.getTableName());
	 logger.info("Persist Dynamo DB2");
	 PutItemOutcome outcome = table.putItem(new PutItemSpec().withItem(
    new Item().withNumber("empId", employee.getEmpId())
               .withString("firstName", employee.getFirstName())
               .withString("lastName", employee.getLastName())));
  logger.info("after Persist Dynamo DB");*/
  //return outcome;
	    return null;
 }
 
 
 
}