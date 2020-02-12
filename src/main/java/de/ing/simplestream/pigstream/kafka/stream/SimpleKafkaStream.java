package de.ing.simplestream.pigstream.kafka.stream;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import de.ing.simplestream.pigstream.kafka.json.KafkaJsonDeserializer;

public class SimpleKafkaStream<T> extends Thread {

	private List<SimpleKafkaStream<T>> streamList;
	private Processor<T> processor = null;
	private final Deserializer<T> deserializer;
	private Writer<T> writer = null;
	private KafkaConsumer<String, T> consumer;

	public SimpleKafkaStream(final String groupID, final String topic, final Class<T> clazz) {

		
		final Properties props = new Properties();

		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", groupID);
		props.put("enable.auto.commit", "false");
		props.put("auto.offset.reset", "latest");
		
		deserializer = new KafkaJsonDeserializer<T>(clazz);
		consumer = new KafkaConsumer<String, T>(props, new StringDeserializer(), deserializer);
		consumer.subscribe(Collections.singletonList(topic));

	}

	public SimpleKafkaStream<T> processor(Processor<T> procesor) {
		this.processor = procesor;
		return this;
	}

	public SimpleKafkaStream<T> writer(Writer<T> writer) {
		this.writer = writer;
		return this;
	}

	@Override
	public void run() {
		try {
			while (true) {
				ConsumerRecords<String, T> records = consumer.poll(Duration.ofMillis(100));

				for (ConsumerRecord<String, T> record : records) {
					T object = record.value();

					if(processor != null)
						object = processor.process(object);
					
					if(writer != null && object != null)
						writer.write(object);
				}
				consumer.commitAsync();
			}
		} finally {
			consumer.close();
		}
	}
}
