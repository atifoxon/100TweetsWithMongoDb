package com.test.xceed;

import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.GroupCommand;
import com.mongodb.Mongo;

import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

public class SummaryOfTweetFromMongoDb {
	
	private static final String ACCESS_TOKEN = "provide access token here";
	private static final String ACCESS_TOKEN_SECRET = "provide access token secret here";
	private static final String CONSUMER_KEY = "provide cosumer key here";
	private static final String CONSUMER_KEY_SECRET = "provide cosumer key secret";
	private static final String MONGO_DB_HOST = "localhost";
	private static final int MONGO_DB_PORT = 27017;
	private static final String MONGO_DB_NAME = "testTweets";
	private static final String MONGO_DB_COLLECTION = "testCollection";
	
	
	public static void main(String[] args){
		
		
		SummaryOfTweetFromMongoDb summary = new SummaryOfTweetFromMongoDb();
		summary.getAndStoreTweetsOnTopic("Pakistan");
		summary.getTweetCountGroupyByUser();
		summary.getTweetCountGroupyByDate();
		summary.getTweetWithRTCountGreaterThan(5);
		
		// For DB cleanup
//		summary.removeRecords();
		
		
	}

	private void getTweetWithRTCountGreaterThan(int gt) {

		Mongo mongo = null;
		try {
			mongo = new Mongo(MONGO_DB_HOST, MONGO_DB_PORT);
			DB db = mongo.getDB(MONGO_DB_NAME);
			DBCollection collection = db.getCollection(MONGO_DB_COLLECTION);
			
			DBCursor cursor = collection.find(new BasicDBObject("rTCount", new BasicDBObject("$gt", gt)));

			DBObject dbObject;
			
			System.out.println("\n\n========================================");
			System.out.println("Displaying Tweet with RTCount > "+gt);
			
			while(cursor.hasNext()){
				dbObject = cursor.next();
				System.out.println("user "+ dbObject.get("user") + "tweet "+dbObject.get("tweet") + " RTCount " + dbObject.get("rTCount"));
			}
			
			System.out.println("========================================\n\n");
			
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} finally{
			if (mongo!= null) mongo.close();
		}
		
	}

	private void getTweetCountGroupyByUser() {
		Mongo mongo = null;
		try {
			mongo = new Mongo(MONGO_DB_HOST, MONGO_DB_PORT);
			DB db = mongo.getDB(MONGO_DB_NAME);
			DBCollection collection = db.getCollection(MONGO_DB_COLLECTION);
			
			GroupCommand groupCommad = new GroupCommand(collection, new BasicDBObject("user", true),  null, new BasicDBObject("count", 0), "function(key,val){ val.count++;}", null);
			
			DBObject obj = collection.group(groupCommad);
						
			Map<Object, Object > map = obj.toMap();
			
			Iterator<Entry<Object, Object>> i = map.entrySet().iterator();
			Entry<Object, Object> entry;
			
			System.out.println("\n\n========================================");
			System.out.println("Displaying summary Tweet count per User");
			
			
			while(i.hasNext()){
				entry = i.next();
				System.out.println(entry.getValue());
			}
			
			System.out.println("========================================\n\n");
			
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} finally{
			if (mongo!= null) mongo.close();
		}
				
	}
	
	private void getTweetCountGroupyByDate() {
		Mongo mongo = null;
		try {
			mongo = new Mongo(MONGO_DB_HOST, MONGO_DB_PORT);
			DB db = mongo.getDB(MONGO_DB_NAME);
			DBCollection collection = db.getCollection(MONGO_DB_COLLECTION);
			
			GroupCommand groupCommad = new GroupCommand(collection, new BasicDBObject("createdAt", true),  null, new BasicDBObject("count", 0), "function(key,val){ val.count++;}", null);
			
			DBObject obj = collection.group(groupCommad);
			
			Map<Object, Object > map = obj.toMap();
			Iterator<Entry<Object, Object>> i = map.entrySet().iterator();
			Entry<Object, Object> entry;
			
			System.out.println("\n\n========================================");
			System.out.println("Displaying summary Tweet count per Date");
			
			
			while(i.hasNext()){
				entry = i.next();
				System.out.println(entry.getValue());
			}
			
			System.out.println("========================================\n\n");
			
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} finally{
			if (mongo!= null) mongo.close();
		}
				
	}

	private void getAndStoreTweetsOnTopic(String topic) {
		
		// Configuring twitter
		ConfigurationBuilder configBuilder = new  ConfigurationBuilder();
		configBuilder.setOAuthAccessToken(ACCESS_TOKEN);
		configBuilder.setOAuthAccessTokenSecret(ACCESS_TOKEN_SECRET);
		configBuilder.setOAuthConsumerKey(CONSUMER_KEY);
		configBuilder.setOAuthConsumerSecret(CONSUMER_KEY_SECRET);
		
		Configuration configuration = configBuilder.build();
		
		TwitterFactory factory = new TwitterFactory(configuration);
		
		Twitter twitter = factory.getInstance();
		
		Query query =  new Query(topic);
		
		//number of tweets to return per page
		query.setCount(100);
		
		
		try {
			
			// search for tweets on specifc topics
			QueryResult search = twitter.search(query);
			
			List<Status> tweets = search.getTweets();
			
			// initialize Mongo DB
			Mongo mongo = new Mongo(MONGO_DB_HOST, MONGO_DB_PORT);
			DB db = mongo.getDB(MONGO_DB_NAME);
			DBCollection collection = db.getCollection(MONGO_DB_COLLECTION);
			
			
			for(Status tweet:tweets){
				
				System.out.println(getTweetInfo(tweet));
				
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");      
			    Date dateWithoutTime = sdf.parse(sdf.format(tweet.getCreatedAt()));
			    
			    // create a document
				BasicDBObject document = new BasicDBObject();
				document.put("createdAt", dateWithoutTime);
				document.put("user", tweet.getUser().getName());
				document.put("rTCount", tweet.getRetweetCount());
				document.put("tweet", tweet.getText());
				document.put("source", tweet.getSource());
				
				// store it in database
				collection.insert(document);
							
			}
			
			/**
			// Find all records from collection
			DBCursor cursorDoc = collection.find();
			
			// ... and display
			while (cursorDoc.hasNext()) {
				System.out.println(cursorDoc.next());
			}**/
			
			
		} catch  (TwitterException e) {
			e.printStackTrace();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
	}

	private String getTweetInfo(Status tweet) {
		return "Tweet by: "+tweet.getUser().getName()+" Tweet: "+tweet.getText()+" Posted at: "+tweet.getCreatedAt()+" Source: "+tweet.getSource()+" RT Count: "+tweet.getRetweetCount()+" Hashtag counts "+tweet.getHashtagEntities().length;
	}
	
	private void removeRecords() {
		Mongo mongo;
		try {
			mongo = new Mongo(MONGO_DB_COLLECTION, MONGO_DB_PORT);
			DB db = mongo.getDB(MONGO_DB_NAME);
			DBCollection collection = db.getCollection(MONGO_DB_COLLECTION);
			
			collection.remove(new BasicDBObject());
			
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		
	}


}
