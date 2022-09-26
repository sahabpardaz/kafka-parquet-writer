package ir.sahab.kafka.reader;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.common.collect.ImmutableMap;
import ir.sahab.kafka.parquet.HdfsTestUtil;
import ir.sahab.kafka.parquet.ParquetTestUtils;
import ir.sahab.kafka.parquet.TemporaryHdfsDirectoryExtension;
import ir.sahab.kafka.reader.KafkaProtoParquetWriter.Builder;
import ir.sahab.kafka.test.proto.TestMessage.SampleMessage;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;

@EmbeddedKafka(count = 1, controlledShutdown = true, topics = "proto-source")
@Timeout(15)
class KafkaProtoParquetWriterJUnit5Test {

    private static final String TOPIC = "proto-source";
    private static final String INSTANCE_NAME = "TestParquetWriter";
    private static final String DEFAULT_PARQUET_FILE_EXTENSION = ".parquet";
    private static final String CONSUMER_GROUP_ID = "CONSUMER_GROUP_ID";
    private static final String OFFSET_RESET_STRATEGY = OffsetResetStrategy.EARLIEST.name().toLowerCase();
    private static final HdfsConfiguration hdfsConfig = new HdfsConfiguration();
    private static final int OFFSET_TRACKER_PAGE_SIZE = 1;
    private static final int DEFAULT_MAX_FILE_OPEN_DURATION_SECONDS = 2;

    private static MiniDFSCluster miniDFSCluster;
    private static EmbeddedKafkaBroker embeddedKafka;

    private ImmutableMap<String, Object> consumerConfig;
    private String targetPath;

    @RegisterExtension
    TemporaryHdfsDirectoryExtension directory = new TemporaryHdfsDirectoryExtension(hdfsConfig);

    @BeforeAll
    static void setUpClass(EmbeddedKafkaBroker embeddedKafkaBroker, @TempDir File miniClusterDataDir) throws Exception {
        KafkaProtoParquetWriterJUnit5Test.embeddedKafka = embeddedKafkaBroker;
        hdfsConfig.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, miniClusterDataDir.getAbsolutePath());
        miniDFSCluster = new MiniDFSCluster.Builder(hdfsConfig).build();
        miniDFSCluster.waitActive();
    }

    @AfterAll
    static void tearDownClass() {
        miniDFSCluster.shutdown();
    }

    @BeforeEach
    void setUp() {
        consumerConfig = ImmutableMap.<String, Object>builder()
                .put(GROUP_ID_CONFIG, CONSUMER_GROUP_ID)
                .put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
                .put(BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString())
                .put(AUTO_OFFSET_RESET_CONFIG, OFFSET_RESET_STRATEGY)
                .build();

        targetPath = directory.getPath().toUri().getPath();
    }

    @AfterEach
    void tearDown() {
        consumeAllRemainingRecords();
    }

    /**
     * Test maxFileOperationDuration configs. It also checks validity of records written to parquet files.
     */
    @Test
    void testMaxOpenDuration() throws Throwable {
        String parquetFileExtension = ".p";
        ArrayList<SampleMessage> messages;

        Builder<SampleMessage> builder = createCommonParquetWriterBuilder()
                .parquetFileExtension(parquetFileExtension);

        try (KafkaProtoParquetWriter<SampleMessage> writer = builder.build()) {
            writer.start();
            // We send few messages (100) that we ensure to the max file size (1 GB) is not reached.
            messages = sendSampleMessages(100);
            // We should have one file, that is closed base on max open duration.
            waitForFiles(1, parquetFileExtension);
        }

        List<LocatedFileStatus> files = findFiles(parquetFileExtension);
        assertEquals(1, files.size());
        assertThat(files.get(0).getLen(), greaterThan(0L));
        // dateTimePattern is not given, checking if file is created at root of target directory
        assertEquals(directory.getPath(), files.get(0).getPath().getParent());

        List<SampleMessage> readRecords = ParquetTestUtils.readParquetFiles(hdfsConfig, files, SampleMessage.class);
        assertEquals(messages.size(), readRecords.size());
        assertThat(messages, containsInAnyOrder(readRecords.toArray(new SampleMessage[0])));
    }

    @Test
    void testMaxFileSize() throws Throwable {
        final int blockSize = 10 * 1024;
        final int maxFileSize = 10 * blockSize;
        final int numberOfFilesWithMaxFileSize = 2;

        Builder<SampleMessage> builder = createCommonParquetWriterBuilder()
                .blockSize(blockSize)
                .maxFileSize(maxFileSize);

        boolean extraFileExists = false;

        try (KafkaProtoParquetWriter<SampleMessage> writer = builder.build()) {
            writer.start();

            // Make sure that Parquet Writer has written the expected files.
            int numMessages = 1000;
            int totalMessagesSent = 0;
            while (findFiles(DEFAULT_PARQUET_FILE_EXTENSION).size() < numberOfFilesWithMaxFileSize) {
                if (writer.getTotalWrittenRecords() != totalMessagesSent) {
                    Thread.sleep(1);
                } else {
                    sendSampleMessages(numMessages);
                    totalMessagesSent += numMessages;
                }
            }

            // If there are some records remained, wait for them to be written too.
            if (writer.getTotalWrittenRecords() != totalMessagesSent) {
                extraFileExists = true;
                waitForFiles(numberOfFilesWithMaxFileSize + 1, DEFAULT_PARQUET_FILE_EXTENSION);
            }
        }

        List<LocatedFileStatus> files = findFiles(DEFAULT_PARQUET_FILE_EXTENSION);

        // Remove the extra file, so we can check the other files that should have maximum size.
        if (extraFileExists) {
            files.sort(Comparator.comparing(LocatedFileStatus::getModificationTime));
            files.remove(files.size() - 1);
        }

        for (LocatedFileStatus file : files) {
            assertThat(file.getLen(), greaterThan(0L));
            double ratioToMaxSize = maxFileSize / (double) file.getLen();
            assertThat(ratioToMaxSize, greaterThan(0.9));
            // File can be a little bigger than maxFileSize as we check the size after writing record
            assertThat(ratioToMaxSize, lessThan(1.01));
        }
    }

    /**
     * Tests whether files are created in directories specified by
     * {@link Builder#directoryDateTimePattern(String)} config.
     */
    @Test
    void testDirectoryDateTimePattern() throws Throwable {
        // Creating and starting parquet writer instance
        String directoryDateTimePattern = "yyyy/dd";

        Builder<SampleMessage> builder = createCommonParquetWriterBuilder()
                .directoryDateTimePattern(directoryDateTimePattern);

        List<SampleMessage> messages;

        try (KafkaProtoParquetWriter<SampleMessage> writer = builder.build()) {
            writer.start();
            // Sending message and waiting for parquet files to be created
            messages = sendSampleMessages(100);
            waitForFiles(1, DEFAULT_PARQUET_FILE_EXTENSION);
        }

        // Checking parquet files are created in correct path
        String expectedDir = DateTimeFormatter.ofPattern(directoryDateTimePattern, Locale.getDefault())
                .withZone(ZoneId.systemDefault())
                .format(Instant.now());

        List<LocatedFileStatus> files = findFiles(DEFAULT_PARQUET_FILE_EXTENSION);

        for (LocatedFileStatus file : files) {
            assertThat(file.getLen(), greaterThan(0L));
            assertEquals(new Path(directory.getPath(), expectedDir), file.getPath().getParent());
        }

        // Checking whether they are stored in parquet files correctly
        List<SampleMessage> actual = ParquetTestUtils.readParquetFiles(hdfsConfig, files, SampleMessage.class);
        assertEquals(messages.size(), actual.size());
        assertThat(actual, containsInAnyOrder(messages.toArray(new SampleMessage[0])));
    }

    /**
     * Consumes the remaining records of the topic and commits the last offset of partitions.
     */
    private void consumeAllRemainingRecords() {
        Properties properties = new Properties();
        properties.put(GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
        properties.put(BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString());
        properties.put(AUTO_OFFSET_RESET_CONFIG, OFFSET_RESET_STRATEGY);
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singleton(TOPIC));
            ConsumerRecords<byte[], byte[]> records;

            do {
                records = consumer.poll(Duration.ofMillis(1_000));
            }
            while (!records.isEmpty());

            consumer.commitSync();
        }
    }

    /**
     * Creates a builder of Parquet writer instance using common configurations.
     */
    private Builder<SampleMessage> createCommonParquetWriterBuilder() {
        return new Builder<SampleMessage>()
                .hadoopConf(hdfsConfig)
                .instanceName(INSTANCE_NAME)
                .topicName(TOPIC)
                .consumerConfig(consumerConfig)
                .maxFileOpenDurationSeconds(DEFAULT_MAX_FILE_OPEN_DURATION_SECONDS)
                .targetDir(targetPath)
                .protoClass(SampleMessage.class)
                .parser(SampleMessage.PARSER)
                .offsetTrackerPageSize(OFFSET_TRACKER_PAGE_SIZE);
    }

    /**
     * Waits till given number of files are created.
     */
    private void waitForFiles(int fileCount, String parquetFileExtension) throws InterruptedException, IOException {
        while (findFiles(parquetFileExtension).size() < fileCount) {
            Thread.sleep(1);
        }
    }

    /**
     * Searches for parquet files in directory specified by {@link Builder#targetDir(String)}.
     *
     * @return list of found files
     */
    private List<LocatedFileStatus> findFiles(String parquetFileExtension) throws IOException {
        return HdfsTestUtil.listFiles(hdfsConfig, directory.getPath(), parquetFileExtension, true);
    }

    /**
     * Send multiple message to Kafka topic.
     *
     * @param count number of message to send
     * @return list of sent message
     */
    private ArrayList<SampleMessage> sendSampleMessages(int count) {
        Map<String, Object> conf = ImmutableMap.<String, Object>builder()
                .put(BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString())
                .build();

        ArrayList<SampleMessage> sampleMessages = new ArrayList<>();

        try (KafkaProducer<Long, byte[]> producer =
                     new KafkaProducer<>(conf, new LongSerializer(), new ByteArraySerializer())) {
            Random random = new Random();
            for (int i = 0; i < count; i++) {
                SampleMessage message = SampleMessage.newBuilder()
                        .setTimestamp(System.currentTimeMillis() + i)
                        .setPageNumber(random.nextInt())
                        .setQuery(RandomStringUtils.random(30))
                        .setResultPerPage(random.nextInt())
                        .build();
                ProducerRecord<Long, byte[]> record = new ProducerRecord<>(TOPIC, (long) i, message.toByteArray());
                producer.send(record, (metadata, exception) -> assertNull(exception));
                sampleMessages.add(message);
            }
        }

        return sampleMessages;
    }
}
