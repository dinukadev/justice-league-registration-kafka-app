package com.jl.registration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
import java.util.Properties;

/**
 * @author dinuka
 * <p>
 * This is the main Kafka Stream application class for the Justice League registration
 */
public class JusticeLeagueRegistrationApplication {


    public static void main(String[] args) {
        new JusticeLeagueRegistrationApplication().process();
    }

    private void process() {
        Properties kafkaProperties = new Properties();
        kafkaProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "jl-reg-app");
        kafkaProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        kafkaProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        kafkaProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        kafkaProperties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        kafkaProperties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

        StreamsBuilder kStreamBuilder = new StreamsBuilder();

        GlobalKTable<String, String> superHeroTable = kStreamBuilder.globalTable("superhero-powers-topic");

        KStream<String, String> superHeroRegistrationStream = kStreamBuilder.stream("jl-reg-input-topic");


        KTable<String, Long> filteredStream = superHeroRegistrationStream
                .selectKey((key, value) -> value)
                .join(superHeroTable, (key, value) -> key,
                        (registration, superHeroPower) -> superHeroPower)
                .flatMapValues(value-> Arrays.asList(value.split(":")))
                .selectKey((key,value)->value).groupByKey().count(Materialized.as("JLPowerCounter"));

        filteredStream.toStream().to("jl-final-topic",Produced.with(Serdes.String(),Serdes.Long()));



        KafkaStreams kafkaStreams = new KafkaStreams(kStreamBuilder.build(), kafkaProperties);
        kafkaStreams.cleanUp();
        kafkaStreams.start();

        System.out.println(kafkaStreams.toString());
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }
}
