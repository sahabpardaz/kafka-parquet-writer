package ir.sahab.kafka.reader;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableMap;
import ir.sahab.kafka.parquet.HdfsTestUtil;
import ir.sahab.kafka.parquet.ParquetTestUtils;
import ir.sahab.kafka.parquet.TemporaryHdfsDirectory;
import ir.sahab.kafka.reader.KafkaProtoParquetWriter.Builder;
import ir.sahab.kafka.test.proto.TestMessage.SampleMessage;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.TimeUnit;
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
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;

public class KafkaProtoParquetWriterTest {

    private static final String TOPIC = "proto-source";
    private static final String INSTANCE_NAME = "TestParquetWriter";
    private static final String DEFAULT_PARQUET_FILE_EXTENSION = ".parquet";
    private static final String CONSUMER_GROUP_ID = "CONSUMER_GROUP_ID";
    private static final String OFFSET_RESET_STRATEGY = OffsetResetStrategy.EARLIEST.name().toLowerCase();
    private static final HdfsConfiguration hdfsConfig = new HdfsConfiguration();
    private static final int OFFSET_TRACKER_PAGE_SIZE = 1;
    private static final int DEFAULT_MAX_FILE_OPEN_DURATION_SECONDS = 2;

    private static MiniDFSCluster miniDFSCluster;

    private ImmutableMap<String, Object> consumerConfig;
    private String targetPath;

    @ClassRule
    public static EmbeddedKafkaRule embeddedKafkaRule = new EmbeddedKafkaRule(1, true, TOPIC);

    @ClassRule
    public static TemporaryFolder miniClusterDataDir = new TemporaryFolder();

    @Rule
    public TemporaryHdfsDirectory directory = new TemporaryHdfsDirectory(hdfsConfig);

    @Rule
    public Timeout timeout = new Timeout(15, TimeUnit.SECONDS);

    @BeforeClass
    public static void setUpClass() throws Exception {
        hdfsConfig.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, miniClusterDataDir.newFolder().getAbsolutePath());
        miniDFSCluster = new MiniDFSCluster.Builder(hdfsConfig).build();
        miniDFSCluster.waitActive();
    }

    @AfterClass
    public static void tearDownClass() {
        miniDFSCluster.shutdown();
    }

    @Before
    public void setUp() {
        consumerConfig = ImmutableMap.<String, Object>builder()
                .put(GROUP_ID_CONFIG, CONSUMER_GROUP_ID)
                .put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
                .put(BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaRule.getEmbeddedKafka().getBrokersAsString())
                .put(AUTO_OFFSET_RESET_CONFIG, OFFSET_RESET_STRATEGY)
                .build();

        targetPath = directory.getPath().toUri().getPath();
    }

    /**
     * Test maxFileOperationDuration configs. It also checks validity of records written to parquet files.
     */
    @Test
    public void testMaxOpenDuration() throws Throwable {
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

        consumeAllRemainingRecords();

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
    public void testMaxFileSize() throws Throwable {
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
                    Thread.sleep(100L);
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

        consumeAllRemainingRecords();

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
    public void testDirectoryDateTimePattern() throws Throwable {
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

        consumeAllRemainingRecords();

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
     * This function commits uncommitted records at the end of the tests.
     * It should be run at the end of the tests to make sure no record of current test appears in the next test.
     */
    private void consumeAllRemainingRecords() {
        Properties properties = new Properties();
        properties.put(GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
        properties.put(BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaRule.getEmbeddedKafka().getBrokersAsString());
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
                .put(BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaRule.getEmbeddedKafka().getBrokersAsString())
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
                producer.send(record, (metadata, exception) -> Assert.assertNull(exception));
                sampleMessages.add(message);
            }
        }

        return sampleMessages;
    }
}
