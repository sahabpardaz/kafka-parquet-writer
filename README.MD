# Kafka Proto Parquet Writer
Kafka proto parquet writer reads records from a Kafka topic and writes them as parquet files. Parquet files can be created on local file system or HDFS. It can write records in multiple threads. As writing to single parquet file can not be done concurrently, each threads writes to a separate file. 

Kafka proto parquet writer uses [Smart Commit Kafka Consumer](https://github.com/sahabpardaz/smart-commit-kafka-consumer) module for reading records from kafka.

Each thread creates new parquet files when certain criteria have been met in output file. Currently these policies are supported for closing a file and opening a new one:

* File Size: When size of the parquet file reaches a threshold
* Record Count: When number of records written to parquet file reaches a threshold
* Open Time: When a file has been open for certain amount of time

These policies can be configured together and file is closed when any of configured threshold has been reached.
## Sample Usage

```
Map<String, Object>  consumerConfig = ImmutableMap.<String, Object>builder()
          .put(ConsumerConfig.GROUP_ID_CONFIG, "sample-groupId"
          .put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                  StringDeserializer.class.getName())
          .put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-sever:9092")
          .put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
          .build();


KafkaProtoParquetWriter<SampleMessage> writer =
        new Builder<>("TestParquetWriter", "topic", consumerConfig, smartCommitOffsetTrackerPageSize,
        smartCommitOffsetTrackerMaxOpenPagesPerPartition, SmartCommitMaxQueuedFiles, "/tmp/parquetFiles",
        SampleMessage.class, SampleMessage.PARSER)
                .threadCount(1)
                .maxRecordsInFile(1_000_000)
                .maxFileOpenDuration(15, TimeUnit.MINUTES)
                .build();
writer.start();
```

## Add it to your project
You can reference to this library by either of java build systems (Maven, Gradle, SBT or Leiningen) using snippets from this jitpack link:
[![](https://jitpack.io/v/sahabpardaz/kafka-parquet-writer.svg)](https://jitpack.io/#sahabpardaz/kafka-parquet-writer)