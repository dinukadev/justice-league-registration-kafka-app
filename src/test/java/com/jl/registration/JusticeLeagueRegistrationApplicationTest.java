package com.jl.registration;

import com.jl.registration.constants.AppConstants;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

/**
 * This class will test the Justice League Registration Kafka Streams application.
 */
public class JusticeLeagueRegistrationApplicationTest {

    private TopologyTestDriver topologyTestDriver;

    private ConsumerRecordFactory<String, String> consumerRecordFactory = new ConsumerRecordFactory<>(new StringSerializer(), new StringSerializer());

    @Before
    public void setUp() {
        Properties kafkaProperties = new Properties();
        kafkaProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "jl-reg-app-test");
        kafkaProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        kafkaProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        kafkaProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        JusticeLeagueRegistrationApplication app = new JusticeLeagueRegistrationApplication();
        topologyTestDriver = new TopologyTestDriver(app.createTopology(), kafkaProperties);
    }

    @After
    public void tearDown() {
        topologyTestDriver.close();
    }

    @Test
    public void shouldRegisterJusticeLeagueMember() {
        prepareGlobalKTable();
        registrationInput("batman");
        registrationInput("superman");
        registrationInput("flash");
        registrationInput("aquaman");


        ProducerRecord<String, Long> output;
        output = readSinkTopic();
        OutputVerifier.compareKeyValue(output, "wealth", 1L);

        output = readSinkTopic();
        OutputVerifier.compareKeyValue(output, "stealth", 1L);

        output = readSinkTopic();
        OutputVerifier.compareKeyValue(output, "strength", 1L);

        output = readSinkTopic();
        OutputVerifier.compareKeyValue(output, "speed", 1L);

        output = readSinkTopic();
        OutputVerifier.compareKeyValue(output, "speed", 2L);

        output = readSinkTopic();
        OutputVerifier.compareKeyValue(output, "timetravel", 1L);

        output = readSinkTopic();
        OutputVerifier.compareKeyValue(output, "fishy", 1L);

        output = readSinkTopic();
        OutputVerifier.compareKeyValue(output, "strength", 2L);
    }

    private ProducerRecord<String, Long> readSinkTopic() {
        return topologyTestDriver.readOutput(AppConstants.JL_FINAL_TOPIC, new StringDeserializer(), new LongDeserializer());
    }

    public void registrationInput(String input) {
        topologyTestDriver.pipeInput(consumerRecordFactory.create(AppConstants.JL_REG_INPUT_TOPIC, null, input));
    }

    private void prepareGlobalKTable() {
        topologyTestDriver.pipeInput(consumerRecordFactory.create(AppConstants.SUPERHERO_POWER_TOPIC, "batman", "wealth:stealth"));
        topologyTestDriver.pipeInput(consumerRecordFactory.create(AppConstants.SUPERHERO_POWER_TOPIC, "superman", "strength:speed"));
        topologyTestDriver.pipeInput(consumerRecordFactory.create(AppConstants.SUPERHERO_POWER_TOPIC, "flash", "speed:timetravel"));
        topologyTestDriver.pipeInput(consumerRecordFactory.create(AppConstants.SUPERHERO_POWER_TOPIC, "aquaman", "fishy:strength"));

    }


}
