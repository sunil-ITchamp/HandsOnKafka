package com.sk.HandsOnKafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class BasicKTable {
    public static void main(String[] args) throws IOException {
        Properties props = new Properties();
        try (FileInputStream fis = new FileInputStream("src/main/resources/streams.properties")) {
            props.load(fis);
        }

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "basic-ktable-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        StreamsBuilder builder = new StreamsBuilder();

        final String orderNumberStart = "orderNumber-";
        KTable<String, String> firstStream = builder.table("my-topic",
                Materialized.<String, String, KeyValueStore<Bytes,byte[]>>as("ktable-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.String()));

        firstStream.filter((k, v) -> v.startsWith(orderNumberStart))
                .mapValues((k, v) -> v.substring(v.indexOf("-")+1))
                .toStream()
                .peek((k, v) -> System.out.println(" +++++++++ = " +  k + " : " + v))
                .to("sk_topic1", Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }


}
