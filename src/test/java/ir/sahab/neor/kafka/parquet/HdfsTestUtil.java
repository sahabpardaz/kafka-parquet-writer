package ir.sahab.neor.kafka.parquet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Contains method used in tests communicating HDFS.
 */
public class HdfsTestUtil {

    private static final int DEFAULT_TIMEOUT = 60_000;
    private static final int DEFAULT_RETRY_WAIT_TIME = 100;

    private static Logger logger = LoggerFactory.getLogger(HdfsTestUtil.class);

    private HdfsTestUtil() {}

    /**
     * Same as waitForSafeModeLeave(DistributedFileSystem, {@link #DEFAULT_TIMEOUT})
     */
    public static void waitForSafeModeLeave(DistributedFileSystem hdfs)
            throws InterruptedException, IOException, TimeoutException {
        waitForSafeModeLeave(hdfs, DEFAULT_TIMEOUT);
    }

    /**
     * Same as waitForSafeModeLeave(DistributedFileSystem, int, {@link #DEFAULT_RETRY_WAIT_TIME})
     */
    public static void waitForSafeModeLeave(DistributedFileSystem hdfs, int timeout)
            throws InterruptedException, TimeoutException, IOException {
        waitForSafeModeLeave(hdfs, timeout, DEFAULT_RETRY_WAIT_TIME);
    }

    /**
     * Waits for an HDFS to leave safe mode.
     *
     * @param hdfs HDFS to check @param timeout max time to wait @param retryWaitTime time to wait
     *        between checks
     * @param timeout time to wait in milliseconds
     * @param retryWaitTime time to wait between retries
     *
     * @throws IOException if connecting namenode fails
     * @throws TimeoutException if timeout happens while waiting
     * @throws InterruptedException if thread is interrupted while sleeping
     */
    public static void waitForSafeModeLeave(DistributedFileSystem hdfs, int timeout,
            int retryWaitTime) throws IOException, TimeoutException, InterruptedException {
        long startMillis = System.currentTimeMillis();
        while (hdfs.isInSafeMode()) {
            if ((System.currentTimeMillis() - startMillis) > timeout) {
                throw new TimeoutException(
                        "Namenode did not leave safe mode in " + timeout + " milliseconds.");
            }
            logger.debug("Waiting for {} to leave safe mode (timeout={}, retryWaitTime={}).", hdfs,
                    timeout, retryWaitTime);
            Thread.sleep(retryWaitTime);
        }
        logger.debug("HDFS {} is out of safe mode.", hdfs);
    }

    /**
     * Searches for files with requested extension in given directory.
     * @param hdfsConfig hadoop configuration to use
     * @param directory path of directory to search in
     * @param extension extension of requested files
     * @param recursive true means subdirectories must be searched recursively
     * @return list of found files
     */
    public static List<LocatedFileStatus> listFiles(Configuration hdfsConfig, Path directory,
            String extension, boolean recursive) throws IOException {
        RemoteIterator<LocatedFileStatus> list =
                FileSystem.get(hdfsConfig).listFiles(directory, recursive);
        List<LocatedFileStatus> files = new ArrayList<>();
        while (list.hasNext()) {
            LocatedFileStatus file = list.next();
            if (file.getPath().getName().endsWith(extension)) {
                files.add(file);
            }
        }
        return files;
    }
}
