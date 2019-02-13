## Introduction

It is a dark time in the Justice League Universe and they are actively recruiting super heroes. Kafka Streams were 
suggested as a good way to start building out this system which is being depicted in this repository.

### Setup

- Create the topics needed to run the application with the following commands to be run on the $KAFKA_HOME/bin directory

```
./kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --config cleanup.policy=compact --topic superhero-powers-topic
./kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic jl-reg-input-topic
./kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1  --topic jl-final-topic
```

- Then we need to pre-populate the data being read by to the GlobaKTable using the topic `superhero-powers-topic` as follows;

```
./kafka-console-producer.sh --broker-list localhost:9092 --topic superhero-powers-topic \
--property parse.key=true \
--property key.separator=,

>batman,wealth:stealth
>superman,speed:strength
>flash,speed:timetravel
>aquaman,strength:talkwithfishies
```

- Then you need to open up a consumer on one terminal to see the output as follows;

```
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic jl-final-topic --from-beginning \
--formatter kafka.tools.DefaultMessageFormatter \
--property print.key=true \
--property print.value=true \
--property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
--property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
```

- Run the Kafka Stream application by running the main class or with `java -jar target/jl-registration-kafka-stream-app-1.0-SNAPSHOT-jar-with-dependencies.jar`

- Finally, create a producer to start producing the input data for the Kafka Stream application as follows;

```
./kafka-console-producer.sh --broker-list localhost:9092 --topic jl-reg-input-topic

>batman
>superman
>flash
>aquman
```
