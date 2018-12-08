package ir.sahab.neor.kafka.reader;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableMap;
import ir.sahab.neor.kafka.parquet.HdfsTestUtil;
import ir.sahab.neor.kafka.parquet.ParquetTestUtils;
import ir.sahab.neor.kafka.parquet.TemporaryHdfsDirectory;
import ir.sahab.neor.kafka.reader.KafkaProtoParquetWriter.Builder;
import ir.sahab.neor.kafka.test.proto.TestMessage.SampleMessage;
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
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
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
import org.springframework.kafka.test.rule.KafkaEmbedded;

public class KafkaProtoParquetWriterTest {

    private static final String TOPIC = "proto-source";
    private static final String INSTANCE_NAME = "TestParquetWriter";
    private static HdfsConfiguration hdfsConfig = new HdfsConfiguration();


    @ClassRule
    public static KafkaEmbedded kafkaEmbedded = new KafkaEmbedded(1, true, TOPIC);

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
    }

    @AfterClass
    public static void tearDownClass() {
        miniDFSCluster.shutdown();
    }

    @Before
    public void setUp() throws IOException {
        consumerConfig = ImmutableMap.<String, Object>builder()
                .put(ConsumerConfig.GROUP_ID_CONFIG, RandomStringUtils.randomAlphabetic(6))
                .put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                     StringDeserializer.class.getName())
                .put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaEmbedded.getBrokersAsString())
                .put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
                .build();
        targetPath = directory.getPath().toUri().getPath();
    }

    /**
     * Tests maxRecordInFile, maxFileOperationDuration configs. It also checks validity of
     * records written to parquet files.
     */
    @Test
    public void testMaxRecordAndMaxOpenDuration() throws Throwable {
        final int maxRecordInFile = 30;
        Builder<SampleMessage> builder =
                new Builder<>("TestParquetWriter", TOPIC, consumerConfig, targetPath,
                              SampleMessage.class, SampleMessage.PARSER).threadCount(1)
                        .maxRecordsInFile(maxRecordInFile)
                        .maxFileOpenDuration(2, TimeUnit.SECONDS)
                        .hadoopConf(hdfsConfig);

        ArrayList<SampleMessage> messages;
        try (KafkaProtoParquetWriter<SampleMessage> writer = builder.build()) {
            writer.start();
            messages = sendSampleMessages(maxRecordInFile + maxRecordInFile / 2);
            // We should have two files, first is full due to max records and the other one is
            // closed base on max open duration.
            waitForFiles(2);
        }

        List<LocatedFileStatus> files = findFiles();
        assertEquals(2, files.size());
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
                new Builder<>(INSTANCE_NAME, TOPIC, consumerConfig, targetPath,
                              SampleMessage.class, SampleMessage.PARSER).threadCount(1)
                                .hadoopConf(hdfsConfig)
                                .blockSize(10 * 1024)
                                .maxFileSize(maxFileSize);
        try (KafkaProtoParquetWriter<SampleMessage> writer = builder.build()) {
            writer.start();
            while (findFiles().size() < 2) {
                sendSampleMessages(messageCount);
            }
        }
        List<LocatedFileStatus> files = findFiles();
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
        final int maxRecordsInFile = 10;
        Builder<SampleMessage> builder =
                new Builder<>(INSTANCE_NAME, TOPIC, consumerConfig, targetPath,
                              SampleMessage.class, SampleMessage.PARSER).threadCount(1)
                        .hadoopConf(hdfsConfig)
                        .maxRecordsInFile(maxRecordsInFile)
                        .maxFileOpenDuration(500, TimeUnit.MILLISECONDS)
                        .directoryDateTimePattern(directoryDateTimePattern);
        final int fileCount = 2;
        List<SampleMessage> messages;
        try (KafkaProtoParquetWriter<SampleMessage> writer = builder.build()) {
            writer.start();
            // Sending message and waiting for parquet files to be created
            messages = sendSampleMessages(fileCount * maxRecordsInFile);
            waitForFiles(fileCount);
        }

        // Checking parquet files are created in correct path
        String expectedDir =
                DateTimeFormatter.ofPattern(directoryDateTimePattern, Locale.getDefault())
                        .withZone(ZoneId.systemDefault())
                        .format(Instant.now());
        List<LocatedFileStatus> files = findFiles();
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
    private void waitForFiles(int fileCount) throws InterruptedException, IOException {
        while (findFiles().size() < fileCount) {
            Thread.sleep(1);
        }
    }

    /**
     * Searches for parquet files in directory specified by {@link Builder#targetDir}.
     *
     * @return list of found files
     */
    private List<LocatedFileStatus> findFiles() throws IOException {
        return HdfsTestUtil.listFiles(hdfsConfig, directory.getPath(),
                                      KafkaProtoParquetWriter.PARQUET_FILE_EXTENSION, true);
    }

    /**
     * Send multiple message to Kafka topic.
     *
     * @param count number of message to send
     * @return list of sent message
     */
    private ArrayList<SampleMessage> sendSampleMessages(int count) throws Throwable {
        Map<String, Object> conf = ImmutableMap.<String, Object>builder()
                .put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaEmbedded.getBrokersAsString())
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
                    (metadata, exception) -> Assert.assertTrue(exception == null));
                list.add(message);
            }
        }
        return list;
    }
}
