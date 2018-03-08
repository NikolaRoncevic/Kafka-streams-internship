import avro.Person;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class ReadAvroExample {

    private final static String INPUT_STREAM = "avro-implementation-person-output";

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG,"avro-read-from-topic1");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,   Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        config.put("schema.registry.url", "http://localhost:8081");
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, Person> topicRead = builder.stream(INPUT_STREAM);
        //topicRead.peek((key,value) -> System.out.println("key : " + key + " Value: " + value));
        topicRead.peek(new ForeachAction<String, Person>() {
            @Override
            public void apply(String key, Person value) {
                System.out.println("key: " + key + " value: " + value);
            }
        });

        KafkaStreams streams = new KafkaStreams(builder.build(),config);
        streams.start();
        System.out.println("Stream started");

    }
}
