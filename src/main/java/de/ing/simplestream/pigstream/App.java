package de.ing.simplestream.pigstream;

import de.ing.simplestream.pigstream.kafka.io.KafkaTopicWriter;
import de.ing.simplestream.pigstream.kafka.stream.SimpleKafkaStream;
import de.ing.simplestream.pigstream.kafka.stream.Writer;
import de.ing.simplestream.pigstream.services.PigProcessor;
import de.ing.simplestream.pigstream.tiere.Schwein;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        new SimpleKafkaStream<Schwein>("MyGroup", "PigCreatedTopic", Schwein.class)
        .processor(new PigProcessor())
        .writer(new KafkaTopicWriter("PigFeededTopic"))
        .start();
    }
}
