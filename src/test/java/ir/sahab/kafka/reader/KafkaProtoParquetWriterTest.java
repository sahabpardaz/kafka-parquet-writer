package ir.sahab.kafka.reader;

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
import ir.sahab.kafkarule.KafkaRule;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
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

public class KafkaProtoParquetWriterTest {

    private static final String TOPIC = "proto-source";
    private static final String INSTANCE_NAME = "TestParquetWriter";
    private static final String DEFAULT_PARQUET_FILE_EXTENSION = ".parquet";
    private static final int DEFAULT_MAX_FILE_OPEN_DURATION_SECONDS = 2;
    private static HdfsConfiguration hdfsConfig = new HdfsConfiguration();


    @ClassRule
    public static KafkaRule kafkaEmbedded = new KafkaRule();

    @ClassRule
    public static TemporaryFolder miniClusterDataDir = new TemporaryFolder();

    private static MiniDFSCluster miniDFSCluster;

    @Rule
    public TemporaryHdfsDirectory directory = new TemporaryHdfsDirectory(hdfsConfig);

    @Rule
    public Timeout timeout = new Timeout(15, TimeUnit.SECONDS);


    private ImmutableMap<String, Object> consumerConfig;
    private String targetPath;

    @BeforeClass
    public static void setUpClass() throws Exception {
        hdfsConfig.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR,
                       miniClusterDataDir.newFolder().getAbsolutePath());
        miniDFSCluster = new MiniDFSCluster.Builder(hdfsConfig).build();
        miniDFSCluster.waitActive();
        kafkaEmbedded.createTopic(TOPIC, 1);
    }

    @AfterClass
    public static void tearDownClass() {
        miniDFSCluster.shutdown();
    }

    @Before
    public void setUp() {
        consumerConfig = ImmutableMap.<String, Object>builder()
                .put(ConsumerConfig.GROUP_ID_CONFIG, RandomStringUtils.randomAlphabetic(6))
                .put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                     StringDeserializer.class.getName())
                .put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaEmbedded.getBrokerAddress())
                .put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
                .build();
        targetPath = directory.getPath().toUri().getPath();
    }

    /**
     * Test maxFileOperationDuration configs. It also checks validity of records written to parquet files.
     */
    @Test
public void testMaxOpenDuration() throws Throwable {
        String parquetFileExtension = ".p";
        Builder<SampleMessage> builder =
                new Builder<SampleMessage>()
                        .hadoopConf(hdfsConfig)
                        .instanceName(INSTANCE_NAME)
                        .topicName(TOPIC)
                        .consumerConfig(consumerConfig)
                        .maxFileOpenDurationSeconds(DEFAULT_MAX_FILE_OPEN_DURATION_SECONDS)
                        .targetDir(targetPath)
                        .parquetFileExtension(parquetFileExtension)
                        .protoClass(SampleMessage.class)
                        .parser(SampleMessage.PARSER);

        ArrayList<SampleMessage> messages;
        try (KafkaProtoParquetWriter<SampleMessage> writer = builder.build()) {
            writer.start();
            // We send few messages (100) that we ensure to the max file size (1 GB) is not reached.
            messages = sendSampleMessages(100);
            // We should have one file, that is closed base on max open duration.
            waitForFiles(1, parquetFileExtension);
        }

        List<LocatedFileStatus> files = findFiles(parquetFileExtension);
        assertEquals(1, files.size());
        for (LocatedFileStatus file : files) {
            assertThat(file.getLen(), greaterThan(0L));
            // dateTimePattern is not given, checking if file is created at root of target directory
            assertEquals(directory.getPath(), file.getPath().getParent());
        }
        List<SampleMessage> readRecords =
                ParquetTestUtils.readParquetFiles(hdfsConfig, files, SampleMessage.class);
        assertEquals(messages.size(), readRecords.size());
        assertThat(messages, containsInAnyOrder(readRecords.toArray(new SampleMessage[0])));
    }

    @Test
    public void testMaxFileSize() throws Throwable {
        final int maxFileSize = 100 * 1024;
        final int messageCount = 1000;
        Builder<SampleMessage> builder =
                new Builder<SampleMessage>()
                        .hadoopConf(hdfsConfig)
                        .instanceName(INSTANCE_NAME)
                        .topicName(TOPIC)
                        .consumerConfig(consumerConfig)
                        .maxFileOpenDurationSeconds(DEFAULT_MAX_FILE_OPEN_DURATION_SECONDS)
                        .targetDir(targetPath)
                        .protoClass(SampleMessage.class)
                        .parser(SampleMessage.PARSER)
                        .blockSize(10 * 1024)
                        .maxFileSize(maxFileSize);
        try (KafkaProtoParquetWriter<SampleMessage> writer = builder.build()) {
            writer.start();
            while (findFiles(DEFAULT_PARQUET_FILE_EXTENSION).size() < 2) {
                sendSampleMessages(messageCount);
            }
        }
        List<LocatedFileStatus> files = findFiles(DEFAULT_PARQUET_FILE_EXTENSION);
        files.sort(Comparator.comparing(LocatedFileStatus::getLen));
        for (LocatedFileStatus file : files) {
            assertThat(file.getLen(), greaterThan(0L));
            double ratioToMaxSize = maxFileSize / (double) file.getLen();
            assertThat(ratioToMaxSize, greaterThan(0.9));
            // File can be a little bigger than maxFileSize as we check the size after writing
            // record
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

        Builder<SampleMessage> builder =
                new Builder<SampleMessage>()
                        .hadoopConf(hdfsConfig)
                        .instanceName(INSTANCE_NAME)
                        .topicName(TOPIC)
                        .consumerConfig(consumerConfig)
                        .maxFileOpenDurationSeconds(DEFAULT_MAX_FILE_OPEN_DURATION_SECONDS)
                        .targetDir(targetPath)
                        .protoClass(SampleMessage.class)
                        .parser(SampleMessage.PARSER)
                        .directoryDateTimePattern(directoryDateTimePattern);
        final int fileCount = 1;
        List<SampleMessage> messages;
        try (KafkaProtoParquetWriter<SampleMessage> writer = builder.build()) {
            writer.start();
            // Sending message and waiting for parquet files to be created
            messages = sendSampleMessages(100);
            waitForFiles(fileCount, DEFAULT_PARQUET_FILE_EXTENSION);
        }

        // Checking parquet files are created in correct path
        String expectedDir =
                DateTimeFormatter.ofPattern(directoryDateTimePattern, Locale.getDefault())
                        .withZone(ZoneId.systemDefault())
                        .format(Instant.now());
        List<LocatedFileStatus> files = findFiles(DEFAULT_PARQUET_FILE_EXTENSION);
        for (LocatedFileStatus file : files) {
            assertThat(file.getLen(), greaterThan(0L));
            assertEquals(new Path(directory.getPath(), expectedDir), file.getPath().getParent());
        }

        // Checking whether they are stored in parquet files correctly
        List<SampleMessage> actual =
                ParquetTestUtils.readParquetFiles(hdfsConfig, files, SampleMessage.class);
        assertEquals(messages.size(), actual.size());
        assertThat(actual, containsInAnyOrder(messages.toArray(new SampleMessage[0])));
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
     * Searches for parquet files in directory specified by {@link Builder#targetDir}.
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
    private ArrayList<SampleMessage> sendSampleMessages(int count) throws Throwable {
        Map<String, Object> conf = ImmutableMap.<String, Object>builder()
                .put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaEmbedded.getBrokerAddress())
                .build();
        ArrayList<SampleMessage> list = new ArrayList<>();
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
                ProducerRecord<Long, byte[]> record =
                        new ProducerRecord<>(TOPIC, (long) i, message.toByteArray());
                producer.send(record,
                    (metadata, exception) -> Assert.assertNull(exception));
                list.add(message);
            }
        }
        return list;
    }
}
