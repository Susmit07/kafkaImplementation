package com.sapient.kafka.KafkaProducerConsumer;

import java.util.Arrays;
import java.util.Properties;
import java.util.function.Supplier;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaConsumerTemplate {


	@SuppressWarnings({ "rawtypes", "unchecked", "resource" })
	public static void main(String[] args) {

		String topicName = "javaMessages";
		String groupName = "javaMessages_Group";

		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, groupName);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

		KafkaConsumer<String, Supplier> consumer = new KafkaConsumer(props);
		consumer.subscribe(Arrays.asList(topicName));

		while(true) {
			// If there nothing to consume the polling will stop automatically after 100 ms.
			ConsumerRecords<String, Supplier> records = consumer.poll(100);
			for(ConsumerRecord<String, Supplier> record : records) {
				System.out.println("Message published from kafka producer: "+record.value());
			}
		}
	}
}
