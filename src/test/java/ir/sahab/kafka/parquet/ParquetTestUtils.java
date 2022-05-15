package ir.sahab.kafka.parquet;

import com.google.protobuf.Message;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.proto.ProtoParquetReader;
import org.apache.parquet.proto.ProtoReadSupport;

/**
 * Contains methods used in tests related to parquet format.
 */
public class ParquetTestUtils {

    private ParquetTestUtils() {
    }

    /**
     * Reads proto messages from given parquet files.
     * @param protoClass class of protocol buffer message to read
     * @return a list of read messages
     */
    public static <T extends Message> List<T> readParquetFiles(Configuration hdfsConfig,
            Collection<LocatedFileStatus> parquetFiles, Class<T> protoClass) throws IOException {
        ProtoReadSupport.setProtobufClass(hdfsConfig, protoClass.getName());
        List<T> logs = new ArrayList<>();
        for (LocatedFileStatus file : parquetFiles) {
            try (ParquetReader<T.Builder> reader =
                    ProtoParquetReader.<T.Builder>builder(file.getPath())
                            .withConf(hdfsConfig)
                            .build()) {

                T.Builder builder;
                while ((builder = reader.read()) != null) {
                    @SuppressWarnings("unchecked")
                    T log = (T) builder.build();
                    logs.add(log);
                }
            }
        }
        return logs;
    }
}
