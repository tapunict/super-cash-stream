package supercashstream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class supercashstream {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "supercashstream");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();
        final ObjectMapper mapper = new ObjectMapper();

        builder.<String, String>stream("cashflow")
               .mapValues((key, value) -> {
                   System.out.println("DEBUG - Key: " + key + " | Value: " + value);
                   try {
                       JsonNode json = mapper.readTree(value);
                       double price = json.get("price").asDouble();
                       int quantity = json.get("quantity").asInt();
                       double revenue = price * quantity;
                       System.out.println("DEBUG - Parsed - Product: " + key + " | Price: " + price + " | Quantity: " + quantity + " | Revenue: " + revenue);
                       return revenue;
                   } catch (Exception e) {
                       System.err.println("ERROR parsing JSON for key " + key + ": " + e.getMessage());
                       System.err.println("ERROR - Raw value: " + value);
                       return 0.0;
                   }
               })
               .groupByKey()
               .reduce(Double::sum, Materialized.<String, Double, KeyValueStore<Bytes, byte[]>>as("revenue-store")
                   .withValueSerde(Serdes.Double()))
               .toStream()
               .to("cashanalytics", Produced.with(Serdes.String(), Serdes.Double()));

        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
