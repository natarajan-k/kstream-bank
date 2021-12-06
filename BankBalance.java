
import java.util.Properties;
import java.util.Arrays;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.time.Instant;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Serialized;

public class BankBalance {
    public static String bootstrapServers;
    public static String sasljaas;
    public static String security;
    public static String saslmechanism;
    public static String topic_in;
    public static String topic_out;
    public static String truststorelocation;
    public static String truststorepassword;
    public static String applicationid;


    public Topology createTopology(){
        StreamsBuilder builder = new StreamsBuilder();
        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
	KStream<String, JsonNode> bankTransactions = builder.stream(topic_in, Consumed.with(Serdes.String(), jsonSerde));

        ObjectNode initialBalance = JsonNodeFactory.instance.objectNode();
        initialBalance.put("count", 0);
        initialBalance.put("balance", 0);
        initialBalance.put("time", Instant.ofEpochMilli(0L).toString());

        KTable<String, JsonNode> bankBalance = bankTransactions
                .groupByKey(Serialized.with(Serdes.String(), jsonSerde))
                .aggregate(
                        () -> initialBalance,
                        (key, transaction, balance) -> newBalance(transaction, balance),
                        Materialized.<String, JsonNode, KeyValueStore<Bytes, byte[]>>as("bank-balance-agg")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(jsonSerde)
                );
        bankBalance.toStream().to(topic_out, Produced.with(Serdes.String(), jsonSerde));
        return builder.build();
    }

    public static void main(String[] args) {
        String propertiesfile = "./config.kafkastream.properties";
        if (args.length == 1) {
          propertiesfile = args[0];
        }

        try (InputStream input = new FileInputStream(propertiesfile)) {
            Properties prop = new Properties();
            prop.load(input);
            bootstrapServers = prop.getProperty("BOOTSTRAP_SERVERS_CONFIG");
            sasljaas = prop.getProperty("SASL_JAAS_CONFIG_CONFIG");
            security = prop.getProperty("SECURITY_PROTOCOL_CONFIG");
            saslmechanism = prop.getProperty("SASL_MECHANISM_CONFIG");
            topic_in = prop.getProperty("topic_in");
	    topic_out = prop.getProperty("topic_out");
            applicationid = prop.getProperty("APPLICATION_ID_CONFIG");
            if (security.contains("SSL")){
                truststorelocation = prop.getProperty("SSL_TRUSTSTORE_LOCATION_CONFIG");
                truststorepassword = prop.getProperty("SSL_TRUSTSTORE_PASSWORD_CONFIG");
            }

        } catch (IOException ex) {
            ex.printStackTrace();
        }
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationid);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp");
//        properties.setProperty("SECURITY_PROTOCOL_CONFIG", security);
        properties.setProperty("security.protocol", security);
//        properties.setProperty("SASL_JAAS_CONFIG_CONFIG", sasljaas);
        properties.setProperty("sasl.jaas.config", sasljaas);
//        properties.setProperty("SASL_MECHANISM_CONFIG", saslmechanism);
        properties.setProperty("sasl.mechanism", saslmechanism);
        if (security.contains("SSL")){
                properties.setProperty("ssl.truststore.location", truststorelocation);
                properties.setProperty("ssl.truststore.password", truststorepassword);
        }
	//This line is for testing purposes only. Avoid caching for demo purposes. Not good for production.
	properties.setProperty(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG,"0");
//	This line to ensure Exactly Once processing
	properties.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG,StreamsConfig.EXACTLY_ONCE);
        BankBalance bankbalance = new BankBalance();

        KafkaStreams streams = new KafkaStreams(bankbalance.createTopology(), properties);
        streams.cleanUp();
        streams.start();
	System.out.println("RAJAN:"+streams.toString());
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static JsonNode newBalance(JsonNode transaction, JsonNode balance) {
        // create a new balance json object
        ObjectNode newBalance = JsonNodeFactory.instance.objectNode();
        newBalance.put("count", balance.get("count").asInt() + 1);
        newBalance.put("balance", balance.get("balance").asInt() + transaction.get("amount").asInt());
        Long balanceEpoch = Instant.parse(balance.get("time").asText()).toEpochMilli();
        Long transactionEpoch = Instant.parse(transaction.get("time").asText()).toEpochMilli();
        Instant newBalanceInstant = Instant.ofEpochMilli(Math.max(balanceEpoch, transactionEpoch));
        newBalance.put("time", newBalanceInstant.toString());
        return newBalance;
    }
}
