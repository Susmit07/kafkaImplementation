package com.sapient.kafka.KafkaProducerConsumer;

import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaProducerTemplate {

	public static void main(String[] args) throws InterruptedException {

		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

		Producer<String, String> producer = new KafkaProducer<String, String>(props) ;

		Random rnd = new Random();
		for (int i = 0; i<10; i++) {
			String publishedMsg = "Java Messages:"+i+":"+rnd.nextInt(100);
			String topicName = "javaMessages";
			ProducerRecord<String, String> data = new ProducerRecord<String, String>(topicName, publishedMsg);
			producer.send(data);
			// Introducing a sleep of 5 seconds so we can display the consumption of the messages by 
			// the consumer simultaneously when both the program are running
			Thread.sleep(5000);
			System.out.println("Data Send succesfully");
		}
		producer.close();
	}
}
