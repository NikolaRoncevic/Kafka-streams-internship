import avro.Person;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;


public class WriteAvroExample {

    private final static String INPUT_TOPIC = "avro-implementation-person-input";
    private final static String OUTPUT_TOPIC = "avro-implementation-person-output";

    public static void main(final String[] args) throws Exception {

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "avro-implementation1");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,   Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        config.put("schema.registry.url", "http://localhost:8081");

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> textLines = builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(),Serdes.String()));

        KStream<String, Person> output = textLines.mapValues((value -> new Person(value,(long)value.length())));

        output.peek((key, value) -> System.out.println("key: " + key + ", value: " + value));
        output.to(OUTPUT_TOPIC);

        KafkaStreams stream = new KafkaStreams(builder.build(),config);
        stream.start();
        System.out.println("Stream started");

    }
}
