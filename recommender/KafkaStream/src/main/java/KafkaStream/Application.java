package KafkaStream;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.Properties;
public class Application {
    public static void main(String[] args) {
        String brokers = "localhost:9092";
        String zookeepers = "localhost:2181";

        // Input and output topic
        String from = "log";
        String to = "recommender";

        // Define kafka streaming configuration
        Properties settings = new Properties();
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "logFilter");
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        settings.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, zookeepers);

        // Create kafka stream configuration object
        StreamsConfig config = new StreamsConfig(settings);

        // Create a topology builder
        TopologyBuilder builder = new TopologyBuilder();

        // Define the topology of stream processing
        builder.addSource("SOURCE", from)
                .addProcessor("PROCESSOR", () -> new LogProcessor(), "SOURCE")
                .addSink("SINK", to, "PROCESSOR");

        KafkaStreams streams = new KafkaStreams(builder, config);

        streams.start();

        System.out.println("Kafka stream started!>>>>>>>>>>>>>>>>>");

    }
}
