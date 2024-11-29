package com.sk.HandsOnKafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class BasicKafkaStreams {
    public static void main(String[] args) throws IOException {
        Properties props = new Properties();
        try (FileInputStream fis = new FileInputStream("src/main/resources/streams.properties")) {
            props.load(fis);
        }

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "basic-streams");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        StreamsBuilder builder = new StreamsBuilder();


        final String orderNumberStart = "orderNumber-";
        final String secondFilter = "car";
        KStream<String, String> firstStream = builder.stream("my-topic", Consumed.with(Serdes.String(), Serdes.String()));
        firstStream.peek((k, v) -> System.out.println(" ========== 1st stream AS-IS ========== " + k + " = " + v))
                .filter((k, v) -> v.startsWith("ORD-"))
                .mapValues((k, v) -> v.substring(v.indexOf("-")+1))
                .peek((k, v) -> System.out.println("++++++++++++ = " + k + " = " + v))
                .to("sk_topic1", Produced.with(Serdes.String(), Serdes.String()));

        KStream<String, String> secondStream = builder.stream("sk_topic1", Consumed.with(Serdes.String(), Serdes.String()));
        secondStream.peek((k, v) -> System.out.println("=========== 2nd Stream AS-IS ========= " + k + " = " + v))
                .filter((k, v) -> v.startsWith("car"))
                .mapValues((k, v) -> v.substring(v.indexOf("-")+1))
                .peek((k, v) -> System.out.println(" ============ 2nd Stream UPDATED ========= key = " + k + "  :: value = " + v))
                .to("car_topic1", Produced.with(Serdes.String(), Serdes.String()));

        KStream<String, String> thirdStream = builder.stream("sk_topic1", Consumed.with(Serdes.String(), Serdes.String()));
        thirdStream.peek((k, v) -> System.out.println("=========== 3rd Stream AS-IS ========= " + k + " = " + v))
                .filter((k, v) -> v.startsWith("jeep"))
                .mapValues((k, v) -> v.substring(v.indexOf("-")+1))
                .peek((k, v) -> System.out.println(" ============ 3rd Stream UPDATED ========= key = " + k + "  :: value = " + v))
                .to("jeep_topic1", Produced.with(Serdes.String(), Serdes.String()));

        Topology topology = builder.build();
//        Topology topology2 = builder2.build();

        System.out.println(topology.describe());
        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();

//        KafkaStreams streams2 = new KafkaStreams(topology2, props);
//        streams.start();
//    }
}
}
