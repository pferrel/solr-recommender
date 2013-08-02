package finderbots.recommenders.hadoop;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: pat
 * Date: 8/1/13
 * Time: 10:34 AM
 * To change this template use File | Settings | File Templates.
 */
public class Utils {
    private static final String DEFAULT_DELIMITER = "\t";
    private static String delimiter = DEFAULT_DELIMITER;

    public static HashBiMap<String, String> readIndex(Path where) throws IOException {
        FileSystem fs = where.getFileSystem(new JobConf());
        FSDataInputStream file = fs.open(where);
        HashBiMap<String, String> biMap = HashBiMap.create();
        BufferedReader bin = new BufferedReader(new InputStreamReader(file));
        String mapEntry;
        while ((mapEntry = bin.readLine()) != null) {
            String[] pair = mapEntry.split(getDelimiter());
            biMap.forcePut(pair[0].trim(), pair[1].trim());
        }
        file.close();
        return biMap;
    }

    public static void writeIndex(BiMap<String, String> map, FSDataOutputStream file) throws IOException {

        for (Map.Entry e : map.entrySet()) {
            file.writeBytes(e.getKey().toString() + getDelimiter() + e.getValue().toString() + "\n");
        }
        file.close();
    }

    //Just in case anyone cares to change the delimiter
    public static String getDelimiter() {
        return delimiter;
    }

    public static void setDelimiter(String newDelimiter) {
        delimiter = newDelimiter;
    }
}
