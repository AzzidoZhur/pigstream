package de.ing.simplestream.pigstream.kafka.io;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import de.ing.simplestream.pigstream.kafka.json.KafkaJsonSerializer;
import de.ing.simplestream.pigstream.kafka.stream.Writer;
import de.ing.simplestream.pigstream.tiere.Schwein;

public class KafkaTopicWriter implements Writer<Schwein>{
	
	private final Producer<String,Schwein> kafkaProducer;
	private final String topic;
	public KafkaTopicWriter(final String topic) {
		final Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		kafkaProducer =   new KafkaProducer<>( props, new StringSerializer(), new KafkaJsonSerializer());
		this.topic = topic;
	}

	@Override
	public void write(Schwein item) {
		kafkaProducer.send(new ProducerRecord<>(topic, item.toString(), item));
	}

}
