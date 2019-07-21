package ir.sahab.kafka.parquet;

import java.io.IOException;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TemporaryHdfsDirectory rule creates a temporary directory on HDFS and automatically deletes it
 * after test completion.
 */
public class TemporaryHdfsDirectory extends ExternalResource {

    private static Logger logger = LoggerFactory.getLogger(TemporaryHdfsDirectory.class);

    /**
     * Number of random numbers used in name of created temporary directory.
     */
    private static final int RANDOM_COUNT = 10;

    private final Configuration config;
    private final Path parentPath;
    private Path path;

    /**
     * Creates a rule which creates a temporary directory inside /tmp directory of HDFS
     * @param config configuration used to connect to HDFS
     */
    public TemporaryHdfsDirectory(Configuration config) {
        this(config,
                new Path(config.get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY) + "/tmp"));
    }

    /**
     * Creates a rule which creates a temporary directory on given path.
     * @param config configuration used to connect to HDFS
     * @param parentPath path to create temporary directory in
     */
    public TemporaryHdfsDirectory(Configuration config, Path parentPath) {
        this.config = config;
        this.parentPath = parentPath;
    }

    /**
     * Creates a random directory in {@link #parentPath}.
     */
    @Override
    protected void before() throws Throwable {
        FileSystem fs = FileSystem.get(config);
        do {
            path = new Path(parentPath,
                            "junit_" + RandomStringUtils.randomAlphanumeric(RANDOM_COUNT));
        } while (fs.exists(path));

        fs.mkdirs(path);
        logger.debug("Temporary directory {} created on {}.", path, fs);
    }

    /**
     * Deletes created temporary directory.
     */
    @Override
    protected void after() {
        try {
            FileSystem.get(config).delete(path, true);
            logger.debug("Temporary directory {} deleted.", path);
        } catch (IOException e) {
            throw new AssertionError("Unable to delete temporary directory: " + path, e);
        }
    }

    /**
     * @return path of created temporary directory
     */
    public Path getPath() {
        return path;
    }
}
