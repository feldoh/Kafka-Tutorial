package com.dexter.learning.kafka_tutorial;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.consumer.TopicFilter;
import kafka.consumer.Whitelist;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.Message;
import kafka.message.MessageAndMetadata;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;

public class KafkaConsumer {
    static private ConsumerConnector consumer;
    static private final SimpleDateFormat dateFormatter = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");

    public static void main(String[] args) {
	Properties consumerConfig = new Properties();
	consumerConfig.put("zk.connect", "hq-dlowe-d01:2181");
	consumerConfig.put("backoff.increment.ms", "100");
	consumerConfig.put("autooffset.reset", "largest");
	consumerConfig.put("groupid", "java-consumer-tutorial");

	consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(consumerConfig));

	// Manual list of topics to read and the number of threads desired (1
	// per partition is good for parallelism)
	/*
	 * Map<String, List<KafkaStream<Message>>> topicStreams =
	 * consumer.createMessageStreams(new HashMap<String, Integer>() {
	 * 
	 * // Double brace initialization creates an anonymous inner class This
	 * // can then be used to write arbitrary code to initialize the object
	 * // This allows initializing even maps! Warning: if used statically //
	 * this will be run before the constructor but after the superclass
	 * 
	 * private static final long serialVersionUID = 1L; { put("pageviews",
	 * 3); put("conversions", 3); } });
	 */
	// Same as above but using a regular expression.
	TopicFilter sourceTopicFilter = new Whitelist("pageviews|conversions|useractivity");
	List<KafkaStream<Message>> streams = consumer.createMessageStreamsByFilter(sourceTopicFilter, 9);
	System.out.println("stand");
	ExecutorService executor = Executors.newFixedThreadPool(streams.size());
	for (final KafkaStream<Message> stream : streams) {
	    executor.submit(new Runnable() {

		@Override
		public void run() {
		    System.out.println("i ran");
		    for (MessageAndMetadata<Message> msgAndMetadata : stream) {
			System.out.println("got message");
			EventMessage event = parseEventMessage(msgAndMetadata);
			if (event != null) {
			    processEventMessage(event);
			}
		    }
		}

	    });
	}
    }

    protected static void processEventMessage(EventMessage event) {
	System.out.println(dateFormatter.format(new Date(event.timestamp)) + " " + event.userUid + " " + event.url);
    }

    protected static EventMessage parseEventMessage(MessageAndMetadata<Message> msgAndMetadata) {
	JsonFactory jsonFactory = new JsonFactory();
	EventMessage event = new EventMessage();

	System.out.println("started parsing");
	ByteBuffer buffer = msgAndMetadata.message().payload();
	byte[] bytes = new byte[buffer.remaining()];
	buffer.get(bytes);
	try {
	    JsonParser jp = jsonFactory.createJsonParser(bytes);
	    while (jp.nextToken() != null) {
		if (jp.getCurrentToken() == JsonToken.FIELD_NAME) {
		    String fieldName = jp.getCurrentName();
		    jp.nextToken();
		    String value = jp.getText();
		    if (fieldName.equals("timestamp")) {
			event.timestamp = Math.round(Double.valueOf(value) * 1000);
		    } else if (fieldName.equals("url")) {
			event.url = value;
		    } else if (fieldName.equals("userUid") && !value.equals("OPT_OUT") && !value.equals("0")) {
			try {
			    event.userUid = UUID.fromString(value);
			} catch (IllegalArgumentException invalidArgument) {
			    event.userUid = null;
			}
		    } else if (fieldName.equals("vdna_widget_mc") && event.userUid == null) {
			try {
			    event.userUid = UUID.fromString(value);
			    /*
			     * TODO replicate the "fake" uuid algo and ignore
			     * those
			     */
			} catch (IllegalArgumentException invalidArgument) {
			    event.userUid = null;
			}
		    }
		    /*
		     * TODO hashmap with ttl of users we've already seen new
		     * restapi.user.UserService userService.get(uuid) ! directly
		     * to cassandr for (WieghtedTag wTag: user.getPersonality()
		     * ) aggregate tags per url and visualise with processing
		     */
		}
	    }
	    jp.close();
	} catch (JsonParseException e) {
	    e.printStackTrace();
	    return null;
	} catch (IOException e) {
	    e.printStackTrace();
	    return null;
	}
	return event;
    }

    public static class EventMessage {
	public Long timestamp;
	public String url;
	public UUID userUid;
    }
}
