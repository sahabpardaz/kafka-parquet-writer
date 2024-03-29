package ir.sahab.kafka.reader;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import ir.sahab.kafka.reader.ParquetFile.ParquetProperties;
import ir.sahab.kafkaconsumer.PartitionOffset;
import ir.sahab.kafkaconsumer.SmartCommitKafkaConsumer;
import java.io.Closeable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Callable;
import org.apache.commons.lang3.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka proto parquet writer reads records from a Kafka topic and writes them as parquet files. Parquet files can be
 * created on local file system or HDFS. It can write records in multiple threads. As writing to single parquet file
 * can not be done concurrently, each threads writes to a separate file.<br>
 * Kafka proto parquet writer uses {@link SmartCommitKafkaConsumer} for reading records from kafka.
 * At least once delivery is guaranteed because the consumer will be notified of a record's ack just if it is
 * written in a parquet file and successfully flushed to the disk.<br>
 * Each thread creates new parquet files when certain criteria have been met in output file.
 * Currently these policies are supported for closing a file and opening a new one:
 * <ul>
 *     <li>
 *         File Size: When size of the parquet file reaches a threshold
 *     </li>
 *     <li>
 *         Open Time: When a file has been open for certain amount of time
 *     </li>
 * </ul>
 *  Names of finalized files will be in timestamp_instanceName_shardIndex.parquet format.<br>
 * Files can optionally be put in separate directories based on the time it has been finalized.
 * Format of these directories can be configured using
 * {@link Builder#directoryDateTimePattern(String)}.
 *
 *
 * @param <T> class of proto message to write
 */
public class KafkaProtoParquetWriter<T extends Message> implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(KafkaProtoParquetWriter.class);

    private static final long RETRY_SLEEP_MILLIS = 100L;

    private static final String TEMP_FILE_EXTENSION = ".tmp";

    private static final Random random = new Random();

    /**
     * Instance name of writer, this value is used for distinguish files generated by different
     * instance of class
     */
    private final String instanceName;
    // Kafka topic to read message from
    private final String topic;
    // The consumer by which we read from input topic
    private final SmartCommitKafkaConsumer<byte[], byte[]> smartCommitKafkaConsumer;
    // Directory to generate parquet files in
    private final Path targetDir;
    // finalized parquet file extension
    private final String parquetFileExtension;
    // Proto message parser used to parse messages
    private final Parser<T> parser;

    // Class of written proto message
    private final Class<T> protoClass;

    // Properties of created parquet files
    private final ParquetProperties parquetProperties;
    // Number of threads used for read and writing
    private final int threadCount;
    // Maximum size of created parquet files. Zero value means no limitation.
    private final long maxFileSize;
    /**
     * Maximum time each file is kept open in seconds, after this time data of current shard
     * will be flushed and next file will be created. Zero value means no limitation.
     */
    private final int maxFileOpenDurationSeconds;

    // Hadoop Configuration
    private final Configuration hadoopConf;
    // Pattern used for directory creation inside targetDir
    private final DateTimeFormatter directoryDateTimeFormatter;
    // Pattern used for final parquet file name creation
    private final DateTimeFormatter fileDateTimeFormatter;

    // Total written records by this writer
    private final Meter totalWrittenRecords = new Meter();
    // Total records written to disk by this writer
    private final Meter totalFlushedRecords = new Meter();
    // Total size of written proto messages in bytes
    private final Meter totalWrittenBytes = new Meter();
    // Total size of flushed data (bytes) in parquet files
    private final Meter totalFlushedBytes = new Meter();
    // Total closed (finalized) parquet files
    private final Meter totalClosedFiles = new Meter();
    // Histogram on size of closed files
    private Histogram fileSizeHistogram;

    private List<WorkerThread> workerThreads;

    private KafkaProtoParquetWriter(Builder<T> builder) {
        instanceName = builder.instanceName;
        topic = builder.topic;
        protoClass = builder.protoClass;
        parser = builder.parser;

        parquetProperties = new ParquetProperties(builder.hadoopConf, builder.blockSize,
                builder.compressionCodecName, builder.pageSize, builder.enableDictionary);
        threadCount = builder.threadCount;
        maxFileSize = builder.maxFileSize;
        maxFileOpenDurationSeconds = builder.maxFileOpenDurationSeconds;
        directoryDateTimeFormatter = builder.directoryDateTimeFormatter;
        fileDateTimeFormatter = builder.fileDateTimeFormatter;

        hadoopConf = new Configuration(builder.hadoopConf);
        String defaultFs = hadoopConf.get(DFSConfigKeys.FS_DEFAULT_NAME_KEY);
        Validate.notNull(defaultFs);
        Validate.isTrue(!defaultFs.isEmpty());
        targetDir = new Path(defaultFs, builder.targetDir);
        parquetFileExtension = builder.parquetFileExtension;

        MetricRegistry metricRegistry = builder.metricRegistry;
        if (metricRegistry != null) {
            metricRegistry.register("parquet.writer.written.records", totalWrittenRecords);
            metricRegistry.register("parquet.writer.flushed.records", totalFlushedRecords);
            metricRegistry.register("parquet.writer.written.bytes", totalWrittenBytes);
            metricRegistry.register("parquet.writer.flushed.bytes", totalFlushedBytes);
            metricRegistry.register("parquet.writer.closed.files", totalClosedFiles);
            fileSizeHistogram = metricRegistry.histogram("parquet.writer.file.size");
        }

        // initializing Kafka consumer
        Properties kafkaConsumerConfig = new Properties();
        kafkaConsumerConfig.putAll(builder.consumerConfig);
        kafkaConsumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        kafkaConsumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        kafkaConsumerConfig.computeIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, key -> "KafkaProtoParquetWriter-" + instanceName);
        smartCommitKafkaConsumer = new SmartCommitKafkaConsumer<>(kafkaConsumerConfig,
                builder.offsetTrackerPageSize,
                builder.offsetTrackerMaxOpenPagesPerPartition,
                builder.maxQueuedRecordsInConsumer);
        smartCommitKafkaConsumer.subscribe(topic);
    }

    /**
     * Starts to poll from Kafka topic and writes the records in parquet files from several concurrent worker threads.
     */
    public void start() {
        logger.info("Starting Kafka parquet writer '{}' for {} topic...", instanceName, topic);
        smartCommitKafkaConsumer.start();
        workerThreads = new ArrayList<>();
        for (int i = 0; i < threadCount; i++) {
            WorkerThread writer = new WorkerThread(i);
            workerThreads.add(writer);
            writer.start();
        }
        logger.info("Kafka parquet writer '{}' started for {} topic.", instanceName, topic);
    }

    /**
     * Closes all parquet files and Kafka consumer. <b>Note that this method does not throw
     * underlying I/O exceptions if closing any of files fails. It is simply logs the exception and
     * returns.</b>
     */
    @Override
    public void close() throws IOException {
        logger.debug("Closing Kafka parquet writer '{}' of {} topic...", instanceName, topic);
        for (WorkerThread worker : workerThreads) {
            worker.close();
        }
        smartCommitKafkaConsumer.close();
        logger.info("Kafka parquet writer '{}' of {} topic closed.", instanceName, topic);
    }

    /**
     * @return number of total written records till writer has been started
     */
    public long getTotalWrittenRecords() {
        return totalWrittenRecords.getCount();
    }

    /**
     * @return size of total proto messages which has been written till writer has been started
     */
    public long getTotalWrittenBytes() {
        return totalWrittenBytes.getCount();
    }

    /**
     * Writing to parquet files are done using instances of this class.
     * Each instance creates thread, reads from Kafka topic and writes them to a parquet file.
     */
    private class WorkerThread implements Runnable, Closeable {

        private final int index;
        private Thread thread;
        private volatile boolean running = false;

        private ParquetFile<T> currentFile;

        /**
         * Each worker thread has a single temporary file path which writes data in it before
         * finalizing the files. On finalizing the files, we rename them to separate names.
         */
        private final Path temporaryFilePath;

        // Lock used to synchronize interrupting thread and closing underlying parquet writer.
        private final Object closeLock = new Object();

        private final ArrayList<PartitionOffset> writtenOffsets = new ArrayList<>();

        WorkerThread(int index) {
            this.index = index;
            String tempDir = targetDir.toString().endsWith("/") ? "tmp" : "/tmp";
            temporaryFilePath =
                    new Path(targetDir + tempDir, instanceName + "_" + index + "_" + random.nextLong() + TEMP_FILE_EXTENSION);
        }

        /**
         * Starts writing received messages to parquet file through. This method is not blocking and reading and
         * writing messages are done in another threads.
         */
        void start() {
            running = true;
            thread = new Thread(this, "KafkaProtoParquetWriter-" + instanceName + "-" + index);
            thread.start();
        }

        @Override
        public void run() {
            while (running) {
                try {
                    if (currentFile != null && isCurrentFileTimedOut()) {
                        finalizeCurrentFile();
                    }
                    ConsumerRecord<?, byte[]> record = smartCommitKafkaConsumer.poll();
                    if (record == null) {
                        Thread.sleep(1);
                        continue;
                    }
                    if (currentFile == null) {
                        currentFile = tryUntilSucceeds(() -> new ParquetFile<>(temporaryFilePath,
                                protoClass, parquetProperties));
                    }
                    T log;
                    try {
                        log = parser.parseFrom(record.value());
                    } catch (InvalidProtocolBufferException e) {
                        // TODO: Support more general purpose way to handle invalid logs
                        // General implementation can call a user provided callback and
                        // expose invalid log metrics
                        throw new IllegalStateException("Invalid proto message received.", e);
                    }
                    tryUntilSucceeds(() -> currentFile.write(log));
                    writtenOffsets.add(new PartitionOffset(record.partition(), record.offset()));
                    totalWrittenRecords.mark();
                    totalWrittenBytes.mark(record.serializedValueSize());
                    if (isCurrentFileFull()) {
                        logger.debug("File {} is full, starting to close file.",
                                temporaryFilePath);
                        finalizeCurrentFile();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    if (running) {
                        throw new IllegalStateException("Unexpected exception occurred.", e);
                    }
                }
            }
        }

        /**
         * @return if open file duration has been reached and current file must be closed.
         */
        private boolean isCurrentFileTimedOut() {
            return (System.currentTimeMillis() - currentFile.getCreationDate().getTime()) / 1000 >
                    maxFileOpenDurationSeconds;
        }

        /**
         * @return true if size or record count has been reached and current file must be flushed
         *         and closed.
         */
        private boolean isCurrentFileFull() {
            return currentFile.getDataSize() >= maxFileSize;
        }

        /**
         * @return name for a new parquet file
         */
        private String newFileName() {
            return (fileDateTimeFormatter == null)
                    ? Instant.now().toEpochMilli() + "_" + instanceName + "_" + index + parquetFileExtension
                    : fileDateTimeFormatter.format(Instant.now()) + "_" + instanceName + "_" + index
                            + parquetFileExtension;
        }

        /**
         * Closes current temporary file and renames it to its final name. It also moves it to
         * appropriate directory based on {@link #directoryDateTimeFormatter} if it has been
         * configured.
         */
        private void finalizeCurrentFile() throws InterruptedException {
            final long dataSize = currentFile.getDataSize();
            // Unfortunately ParquetWriter.close() eats InterruptedException and throws
            // IOException instead. We retry operation when IOException is thrown, this leads to
            // NullPointerException in ParquetWriter.close() in second invocation. To avoid this
            // situation, interrupting ParquetWriter.close() is prevented by synchronizing on a lock
            // object.
            tryUntilSucceeds(() -> {
                synchronized (closeLock) {
                    currentFile.close();
                }
            });
            totalFlushedRecords.mark(currentFile.getNumWrittenRecords());
            totalFlushedBytes.mark(dataSize);
            totalClosedFiles.mark();
            if(fileSizeHistogram != null) {
                fileSizeHistogram.update(dataSize);
            }
            currentFile = null;
            renameAndMoveTempFile();

            // Inform Kafka consumer about the records that are written. So it detects the offsets which can be
            // committed on each partition.
            if (!writtenOffsets.isEmpty()) {
                writtenOffsets.forEach(smartCommitKafkaConsumer::ack);
                writtenOffsets.clear();
            }
        }

        /**
         * Renames temporary file to its final name and optionally moves it to directory configured
         * with {@link #directoryDateTimeFormatter}
         *
         * @return path of final parquet file
         */
        private void renameAndMoveTempFile() throws InterruptedException {
            Path destDir;
            FileSystem fileSystem = tryUntilSucceeds(() -> FileSystem.get(hadoopConf));
            // Creating directory based on provided directory datetime pattern if it does not exist
            if (directoryDateTimeFormatter != null) {
                destDir = new Path(targetDir, directoryDateTimeFormatter.format(Instant.now()));
                tryUntilSucceeds(() -> fileSystem.mkdirs(destDir));
            } else {
                destDir = targetDir;
            }
            // Renaming temporary file based on current time (and optionally moving to appropriate
            // directory if directoryDateTimeFormatter is provided)
            Path finalFilePath = tryUntilSucceeds(() -> {
                Path dst = new Path(destDir, newFileName());
                fileSystem.rename(temporaryFilePath, dst);
                return dst;
            });
            logger.debug("Parquet file '{}' finalized to '{}'", temporaryFilePath, finalFilePath);
        }

        @Override
        public void close() {
            logger.info("Closing parquet writer worker thread: {}.", index);
            running = false;
            if (thread != null) {
                // Preventing interrupting ParquetWriter.close() method. Refer to
                // finalizeCurrentFile method to further explanation.
                synchronized (closeLock) {
                    thread.interrupt();
                }
                try {
                    thread.join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(
                            "Unexpected interrupt while joining thread.", e);
                }
            }
            logger.info("Parquet writer worker thread {} closed.", index);
        }
    }

    /**
     * Tries given I/O operation until succeeds. Operation is retried if {@link IOException} is
     * thrown. It sleeps RETRY_SLEEP_MILLIS milliseconds between retries.
     *
     * @return result of given callable
     *
     * @throws IllegalStateException if operation throws any checked exception other than
     *         {@link IOException}
     */
    private static <V> V tryUntilSucceeds(Callable<V> operation) throws InterruptedException {
        int tried = 0;
        do {
            try {
                return operation.call();
            } catch (InterruptedIOException e) {
                logger.info("I/O operation interrupted.", e);
                throw new InterruptedException("I/O operation interrupted.");
            } catch (IOException e) {
                tried++;
                logger.error("I/O operation failed (tried {}).", tried, e);
                Thread.sleep(RETRY_SLEEP_MILLIS);
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new IllegalStateException("Unexpected exception occurred.", e);
            }
        } while (true);
    }

    /**
     * Tries given I/O operation until succeeds. Operation is retried if {@link IOException} is
     * thrown. It sleeps RETRY_SLEEP_MILLIS milliseconds between retries.
     *
     * @throws IllegalStateException if operation throws any checked exception other than
     *         {@link IOException}
     */
    private static void tryUntilSucceeds(RunnableWithException<?> runnable)
            throws InterruptedException {
        tryUntilSucceeds(() -> {
            runnable.run();
            return null;
        });
    }

    /**
     * Builder class for {@link KafkaProtoParquetWriter}.
     *
     * @param <T> type of proto message
     */
    public static class Builder<T extends Message> {

        // Minimum value for maxFileSize config
        private static final long MIN_MAX_FILE_SIZE = 100 * 1024L;

        //Configs related to parquet writer
        private String instanceName = "parquet-writer";
        private int threadCount = 1;
        private MetricRegistry metricRegistry = null;

        // Configs indicating when should close a parquet file
        private int maxFileOpenDurationSeconds = 15 * 60; // Default: 15 Minutes
        private long maxFileSize = 1024L * 1024 * 1024; // Default: 1GB
        private int maxExpectedThroughputPerSecond = 300_000;

        // Configs about the underlying smart Kafka consumer
        private int offsetTrackerPageSize = 300_000;
        private int offsetTrackerMaxOpenPagesPerPartition = 0; // If not set, it will be selected based on other configs
        private int maxQueuedRecordsInConsumer = 100_000;
        private Map<String, Object> consumerConfig;
        private String topic;

        // Configs related to parquet blocks
        private long blockSize = ParquetWriter.DEFAULT_BLOCK_SIZE;
        private int pageSize = ParquetWriter.DEFAULT_BLOCK_SIZE;

        // Configs related to hdfs
        private Configuration hadoopConf = new Configuration();

        // Configs related to parsing proto
        private Class<T> protoClass;
        private Parser<T> parser;

        //Configs related to parquet files
        private CompressionCodecName compressionCodecName = CompressionCodecName.UNCOMPRESSED;
        private DateTimeFormatter directoryDateTimeFormatter;
        private DateTimeFormatter fileDateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmssSSS",
                Locale.getDefault()).withZone(ZoneId.systemDefault());
        private String parquetFileExtension = ".parquet";
        private boolean enableDictionary = true;
        private String targetDir;

        public Builder() {}

        /**
         * @param instanceName name of this parquet writer instance
         * If it is not set, its default value will be used.
         */
        public Builder<T> instanceName(String instanceName) {
            Validate.notEmpty(instanceName, "instance name can not be null/empty");
            this.instanceName = instanceName;
            return this;
        }

        /**
         * Sets date pattern used to create parquet file. This pattern is required if
         * the parquet files should be categorized according to their time (time of closing a file).
         * @param fileDateTimePattern pattern used to create parquet file from, see {@link DateTimeFormatter}
         * for format. if set to null then we use epoch of time in file name;
         * @throws IllegalArgumentException if pattern is invalid
         */
        public Builder<T> fileDateTimePattern(String fileDateTimePattern) {
            this.fileDateTimeFormatter = (fileDateTimePattern == null)
                    ? null
                    : DateTimeFormatter.ofPattern(fileDateTimePattern, Locale.getDefault())
                            .withZone(ZoneId.systemDefault());
            return this;
        }

        /**
         * @param parquetFileExtension used when we closing one parquet file
         * If it is not set, its default value will be used.
         */
        public Builder<T> parquetFileExtension(String parquetFileExtension) {
            Validate.notEmpty(parquetFileExtension, "file extension can not be null/empty");
            this.parquetFileExtension = parquetFileExtension;
            return this;
        }

        /**
         * @param threadCount number of concurrent threads used for writing parquet file. Default value is 1.
         * If it is not set, its default value will be used.
         */
        public Builder<T> threadCount(int threadCount) {
            Validate.isTrue(threadCount > 0, "Thread count must be a positive number.");
            this.threadCount = threadCount;
            return this;
        }

        /**
         * @param metricRegistry metric registry to register metrics on.
         */
        public Builder<T> metricRegistry(MetricRegistry metricRegistry) {
            this.metricRegistry = metricRegistry;
            return this;
        }

        /**
         * @param maxFileOpenDurationSeconds maximum time each file is kept open in seconds, after this
         * time data of current file will be flushed and next file will be created.
         * If it is not set, its default value will be used.
         */
        public Builder<T> maxFileOpenDurationSeconds(int maxFileOpenDurationSeconds) {
            Validate.isTrue(maxFileOpenDurationSeconds > 0, "Maximum duration of file must be positive number.");
            this.maxFileOpenDurationSeconds = maxFileOpenDurationSeconds;
            return this;
        }

        /**
         * @param maxFileSize maximum size of parquet files. Zero means no limitation. Default value is 0. <br/>
         * Due to file format overheads this value must be >= {{@link #MIN_MAX_FILE_SIZE}}
         * If it is not set, its default value will be used.
         */
        public Builder<T> maxFileSize(long maxFileSize) {
            Validate.isTrue(maxFileSize >= MIN_MAX_FILE_SIZE);
            this.maxFileSize = maxFileSize;
            return this;
        }

        /**
         * @param maxExpectedThroughputPerSecond maximum expected records that may be processed per second.
         * This value does not cause any functional behavior. It is just used to check validity of other performance
         * related configs. In order to avoid temporary pause due to saturation of offset tracker pages,
         * this equation should be satisfied: {@code offsetTrackerPageSize * offsetTrackerMaxOpenPagesPerPartition >=
         * maxExpectedThroughputPerSecond * maxFileOpenDurationSeconds}
         * If it is not set, its default value will be used.
         */
        public Builder<T> maxExpectedThroughputPerSecond(int maxExpectedThroughputPerSecond) {
            Validate.isTrue(maxExpectedThroughputPerSecond > 0, "Maximum expected throughput"
                    + " must be positive number.");
            this.maxExpectedThroughputPerSecond = maxExpectedThroughputPerSecond;
            return this;
        }

        /**
         * @param offsetTrackerPageSize is used in smart kafka consumer, the size of each page in offset tracker.
         * Offsets will be committed just when some consecutive pages become fully acked. In fact lower page sizes,
         * causes more frequent commits.
         * If it is not set, its default value will be used.
         */
        public Builder<T> offsetTrackerPageSize(int offsetTrackerPageSize) {
            Validate.isTrue(offsetTrackerPageSize > 0, "offset tracker page size must be positive number.");
            this.offsetTrackerPageSize = offsetTrackerPageSize;
            return this;
        }

        /**
         * @param offsetTrackerMaxOpenPagesPerPartition is used in smart kafka consumer, maximum number of open
         * pages (pages which have tracked but not acked offsets). After reaching to this limit on a partition,
         * reading from Kafka topic will be blocked, waiting for receiving more pending acks from the client.
         * A good choice is to completely avoid this kind of blockage. For this reason, it is
         * sufficient to satisfy this equation:
         * <pre> (pageSize * maxOpenPages * numPartitions) > (maximum number of pending records) </pre>
         * In the above equation, by pending records we mean the ones which are polled but not yet acked.
         * Note: if its value is not provided by user, it will be chosen automatically based on other parameters.
         */
        public Builder<T> offsetTrackerMaxOpenPagesPerPartition(int offsetTrackerMaxOpenPagesPerPartition) {
            Validate.isTrue(offsetTrackerMaxOpenPagesPerPartition >= 0,
                    "Maximum open pages per partition for offset tracker cannot be negative number.");
            this.offsetTrackerMaxOpenPagesPerPartition = offsetTrackerMaxOpenPagesPerPartition;
            return this;
        }

        /**
         * @param maxQueuedRecordsInConsumer is used in smart kafka consumer, maximum number of records
         * which can be queued to be later polled by the client.
         * If it is not set, its default value will be used.
         */
        public Builder<T> maxQueuedRecordsInConsumer(int maxQueuedRecordsInConsumer) {
            Validate.isTrue(maxQueuedRecordsInConsumer > 0, "Maximum queued records in consumer cannot be negative.");
            this.maxQueuedRecordsInConsumer = maxQueuedRecordsInConsumer;
            return this;
        }

        /**
         * @param consumerConfig config for Kafka consumer
         */
        public Builder<T> consumerConfig(Map<String, Object> consumerConfig) {
            Validate.notEmpty(consumerConfig, "Consumer config cannot be null/empty.");
            this.consumerConfig = consumerConfig;
            return this;
        }

        /**
         * @param topic Kafka topic to read data from
         */
        public Builder<T> topicName(String topic) {
            Validate.notEmpty(topic, "Kafka topic name cannot be null/empty.");
            this.topic = topic;
            return this;
        }

        /**
         * @param blockSize HDFS block size of parquet files
         * If it is not set, its default value will be used.
         */
        public Builder<T> blockSize(long blockSize) {
            Validate.isTrue(blockSize > 0, "Block size must be a positive number.");
            this.blockSize = blockSize;
            return this;
        }

        /**
         * @param pageSize page size of generated parquet files
         * If it is not set, its default value will be used.
         */
        public Builder<T> pageSize(int pageSize) {
            Validate.isTrue(pageSize > 0, "Page size must be a positive number.");
            this.pageSize = pageSize;
            return this;
        }

        public Builder<T> hadoopConf(Configuration hadoopConf) {
            Validate.notNull(hadoopConf);
            this.hadoopConf = hadoopConf;
            return this;
        }

        /**
         * @param protoClass class of proto message to store
         */
        public Builder<T> protoClass(Class<T> protoClass) {
            Validate.notNull(protoClass);
            this.protoClass = protoClass;
            return this;
        }

        /**
         * @param parser parser instance for parsing proto messages
         */
        public Builder<T> parser(Parser<T> parser) {
            Validate.notNull(parser);
            this.parser = parser;
            return this;
        }

        /**
         * @param compressionCodecName name of compression codec used in parquet files
         * If it is not set, its default value will be used.
         */
        public Builder<T> compressionCodecName(CompressionCodecName compressionCodecName) {
            Validate.notNull(compressionCodecName, "Compression codec cannot be null.");
            this.compressionCodecName = compressionCodecName;
            return this;
        }

        /**
         * Sets date pattern used to create directory inside target directory. This pattern is required if
         * the parquet files should be categorized according to their time (time of closing a file).
         * @param directoryDateTimePattern pattern used to create directory from, see {@link DateTimeFormatter}
         * for format. null value disables directory creation.
         * @throws IllegalArgumentException if pattern is invalid
         */
        public Builder<T> directoryDateTimePattern(String directoryDateTimePattern) {
            this.directoryDateTimeFormatter = (directoryDateTimePattern == null)
                    ? null
                    : DateTimeFormatter.ofPattern(directoryDateTimePattern, Locale.getDefault())
                            .withZone(ZoneId.systemDefault());
            return this;
        }

        /**
         * @param enableDictionary whether to enable dictionary in parquet files or not
         * If it is not set, its default value will be used.
         */
        public Builder<T> enableDictionary(boolean enableDictionary) {
            this.enableDictionary = enableDictionary;
            return this;
        }

        /**
         * @param targetDir path of directory to store parquet files
         */
        public Builder<T> targetDir(String targetDir) {
            this.targetDir = targetDir;
            return this;
        }

        public KafkaProtoParquetWriter<T> build() {
            Validate.notEmpty(consumerConfig, "consumer config must be set.");
            Validate.notEmpty(topic, "kafka topic name must be set.");
            Validate.notNull(protoClass, "Proto message class must be set.");
            Validate.notNull(parser, "Proto message parser must be set.");
            Validate.notEmpty(targetDir, "target directory must be set");

            if (offsetTrackerMaxOpenPagesPerPartition == 0) {
                // Calculate the number of offset tracker maximum pages per partition
                offsetTrackerMaxOpenPagesPerPartition = (int) Math.ceil(
                        maxExpectedThroughputPerSecond * maxFileOpenDurationSeconds * 1.0 / offsetTrackerPageSize);
            } else {
                Validate.isTrue(offsetTrackerPageSize * offsetTrackerMaxOpenPagesPerPartition >=
                                maxExpectedThroughputPerSecond * maxFileOpenDurationSeconds,
                        "\"offsetTrackerPageSize * offsetTrackerMaxOpenPagesPerPartition >= "
                                + "maxExpectedThroughputPerSecond * maxFileOpenDurationSeconds\""
                                + "\n above expression must be satisfied to avoid temporary pause due to"
                                + " saturation of offset tracker pages.");
            }
            return new KafkaProtoParquetWriter<>(this);
        }
    }

    /**
     * Similar to {@link Runnable} which can throw exception.
     * @param <X> type of exception that can be thrown
     */
    @FunctionalInterface
    private interface RunnableWithException<X extends Exception> {
        void run() throws X;
    }
}
