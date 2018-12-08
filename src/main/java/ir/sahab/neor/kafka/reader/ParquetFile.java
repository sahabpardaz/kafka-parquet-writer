package ir.sahab.neor.kafka.reader;

import com.google.protobuf.Message;
import java.io.IOException;
import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileWriter.Mode;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.proto.ProtoWriteSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Instance of this class abstracts a parquet file which contains logs in format of protocol
 * buffers.
 * <p/>
 * Instances of this class are not thread-safe.
 *
 * @param <T> type of proto message that file contains
 */
public class ParquetFile<T extends Message> implements AutoCloseable {

    private static Logger logger = LoggerFactory.getLogger(ParquetFile.class);

    private final Path filePath;
    private final Class<T> protoClass;
    private ParquetWriter<T> writer;

    private final Date creationDate;

    private long numWrittenRecords;

    ParquetFile(Path filePath, Class<T> protoClass, ParquetProperties properties)
            throws IOException {
        this.filePath = filePath;
        this.protoClass = protoClass;
        this.creationDate = new Date();

        ParquetWriterBuilder builder =
                new ParquetWriterBuilder(filePath).withConf(properties.hadoopConf)
                        .withRowGroupSize(properties.blockSize)
                        .withCompressionCodec(properties.compressionCodecName)
                        .withWriteMode(Mode.OVERWRITE)
                        .withPageSize(properties.pageSize);
        if (properties.enableDictionary) {
            builder.enableDictionaryEncoding();
        }
        writer = builder.build();

        logger.info("Parquet file {} created.", filePath);
    }

    /**
     * Writes record into underlying parquet file.
     */
    public void write(T record) throws IOException {
        writer.write(record);
        numWrittenRecords++;
    }

    @Override
    public void close() throws IOException {
        writer.close();
        logger.info("Parquet file {} closed (records={}).", filePath, numWrittenRecords);
    }

    public Date getCreationDate() {
        return creationDate;
    }

    /**
     * @return current size of parquet file in bytes
     */
    public long getDataSize() {
        return writer.getDataSize();
    }

    public long getNumWrittenRecords() {
        return numWrittenRecords;
    }

    private class ParquetWriterBuilder extends ParquetWriter.Builder<T, ParquetWriterBuilder> {

        private ParquetWriterBuilder(Path file) {
            super(file);
        }

        @Override
        protected ParquetWriterBuilder self() {
            return this;
        }

        @Override
        protected WriteSupport<T> getWriteSupport(Configuration conf) {
            return new ProtoWriteSupport<>(protoClass);
        }
    }

    /**
     * Instance of this class can be used to specify properties of created parquet files.
     */
    public static class ParquetProperties {

        private final Configuration hadoopConf;

        private final int blockSize;
        private final CompressionCodecName compressionCodecName;
        private final int pageSize;
        private final boolean enableDictionary;

        public ParquetProperties(Configuration hadoopConf, int blockSize,
                CompressionCodecName compressionCodecName, int pageSize, boolean enableDictionary) {
            this.hadoopConf = hadoopConf;
            this.blockSize = blockSize;
            this.compressionCodecName = compressionCodecName;
            this.pageSize = pageSize;
            this.enableDictionary = enableDictionary;
        }
    }
}
