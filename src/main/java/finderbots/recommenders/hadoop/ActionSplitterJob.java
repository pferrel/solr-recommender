package finderbots.recommenders.hadoop;

/**
 * Licensed to Patrick J. Ferrel (PJF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. PJF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * If someone wants license or copyrights to this let me know
 * pat.ferrel@gmail.com
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.mahout.common.HadoopUtil;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * <p>Recursively searches a directory tree for files that contain the string passed in with options. These files may be in HDFS and may be written to HDFS. They are expected to contain tab or comma separated values whose columns have userID, action, and itemID strings. The file will be split into output files one per action desired. Unrecognized output will be put in an 'other' directory. The split files are of the form to be fed to recommender training jobs.
 * <p/>
 * <p>This job creates a single item and user ID space by accumulating all IDs into two BiMaps. The
 * BiMaps are written out to index files for later lookup. This job
 * can scale only so far as memory allows and is single threaded.
 * <p/>
 * <p>Danger: This job overwrites anything in the output directories by default, you will not be warned.
 * <p/>
 * <p>Todo: mapreduce this job so its not memory limited.</p>
 */

public class ActionSplitterJob extends Configured implements Tool {
    private static Logger LOGGER = Logger.getRootLogger();

    private BiMap<String, String> userIndex;
    private BiMap<String, String> itemIndex;
    private static Options options;


    public void split(Path baseInputDir, Path baseOutputDir) throws IOException {
        FileSystem fs = baseInputDir.getFileSystem(getConf());
        Path action1DirPath = new Path(baseOutputDir, options.getAction1Dir());
        Path action2DirPath = new Path(baseOutputDir, options.getAction2Dir());
        Path actionOtherDirPath = new Path(baseOutputDir, options.getActionOtherDir());
        Path action1FilePath = new Path(action1DirPath, options.getAction1File());
        Path action2FilePath = new Path(action2DirPath, options.getAction2File());
        Path actionOtherFilePath = new Path(actionOtherDirPath, options.getActionOtherFile());
        FSDataOutputStream action1File;
        FSDataOutputStream action2File;
        FSDataOutputStream actionOtherFile;

        if (!fs.exists(baseOutputDir)) {
            LOGGER.info("Preference output dir:" + baseOutputDir.toString() + " does not exist. creating it.");
            fs.mkdirs(baseOutputDir);
        }

        if (fs.exists(action1DirPath)) fs.delete(action1DirPath, true);
        if (fs.exists(action2DirPath)) fs.delete(action2DirPath, true);
        if (fs.exists(actionOtherDirPath)) fs.delete(actionOtherDirPath, true);

        // cleaned out prefs if they existed, now create a place to put the new ones
        fs.mkdirs(action1DirPath);
        fs.mkdirs(action2DirPath);
        fs.mkdirs(actionOtherDirPath);
        action1File = fs.create(action1FilePath);
        action2File = fs.create(action2FilePath);
        actionOtherFile = fs.create(actionOtherFilePath);

        List<FSDataInputStream> actionFiles = getActionFiles(baseInputDir);

        Integer uniqueUserIDCounter = 0;
        Integer uniqueItemIDCounter = 0;
        for (FSDataInputStream stream : actionFiles) {
            BufferedReader bin = new BufferedReader(new InputStreamReader(stream));
            String actionLogLine;
            while ((actionLogLine = bin.readLine()) != null) {//get user to make a rec for
                String[] columns = actionLogLine.split(options.getInputDelimiter());
                if (options.getTimestampColumn() != -1) { // ignoring for now but may be useful
                    String timestamp = columns[options.getTimestampColumn()].trim();
                }
                String externalUserIDString = columns[options.getUserIdColumn()].trim();
                String externalItemIDString = columns[options.getItemIdColumn()].trim();
                String actionString = columns[options.getActionColumn()].trim();

                // create a bi-directional index of external->internal ids
                String internalUserID;
                String internalItemID;
                if (this.userIndex.containsKey(externalUserIDString)) {// already in the user index
                    internalUserID = this.userIndex.get(externalUserIDString);
                } else {
                    internalUserID = uniqueUserIDCounter.toString();
                    this.userIndex.forcePut(externalUserIDString, internalUserID);
                    uniqueUserIDCounter += 1;
                    if (uniqueUserIDCounter % 10000 == 0)
                        LOGGER.debug("Splitter processed: " + Integer.toString(uniqueUserIDCounter) + " unique users.");
                }
                if (this.itemIndex.containsKey(externalItemIDString)) {// already in the item index
                    internalItemID = this.itemIndex.get(externalItemIDString);
                } else {
                    internalItemID = uniqueItemIDCounter.toString();
                    this.itemIndex.forcePut(externalItemIDString, internalItemID);
                    uniqueItemIDCounter += 1;
                }
                if (actionString.equals(options.getAction1())) {
                    action1File.writeBytes(internalUserID + options.getOutputDelimiter() + internalItemID + options.getOutputDelimiter() + "1.0\n");
                } else if (actionString.equals(options.getAction2())) {
                    action2File.writeBytes(internalUserID + options.getOutputDelimiter() + internalItemID + options.getOutputDelimiter() + "1.0\n");
                } else {
                    actionOtherFile.writeBytes(actionLogLine);//write what's not recognized
                }
            }
        }
        action1File.close();
        action2File.close();
        actionOtherFile.close();
        int i = 0;//breakpoint after close to inspect files
    }

    public void saveIndexes(Path where) throws IOException {
        Path userIndexPath = new Path(where, options.getUserIndexFile());
        Path itemIndexPath = new Path(where, options.getItemIndexFile());
        FileSystem fs = where.getFileSystem(new JobConf());
        if (fs.getFileStatus(where).isDir()) {
            FSDataOutputStream userIndexFile = fs.create(userIndexPath);
            Utils.writeIndex(userIndex, userIndexFile);
            FSDataOutputStream itemIndexFile = fs.create(itemIndexPath);
            Utils.writeIndex(itemIndex, itemIndexFile);
        } else {
            throw new IOException("Bad locaton for ID Indexes: " + where.toString());
        }
    }

    public int getNumberOfUsers() {
        return this.userIndex.size();
    }

    public int getNumberOfItems() {
        return this.itemIndex.size();
    }

    public List<FSDataInputStream> getActionFiles(Path baseInputDir) throws IOException {
        //this excludes certain hadoop created file in the dirs it looks into
        //todo: should pass a pattern for identifying files to analyze?
        List<FSDataInputStream> files = new ArrayList<FSDataInputStream>();
        FileSystem fs = baseInputDir.getFileSystem(getConf());
        if (fs.getFileStatus(baseInputDir).isDir()) {
            FileStatus[] stats = fs.listStatus(baseInputDir);
            for (FileStatus fstat : stats) {
                if (fstat.isDir()) {
                    files.addAll(getActionFiles(fstat.getPath()));
                    //todo: this should be done with a regex
                } else if (
                    fstat.getPath().getName().contains(options.getInputFilePattern())
                        && !fstat.isDir()
                        && !fstat.getPath().getName().startsWith("_")
                        && !fstat.getPath().getName().startsWith(".")
                    ) {
                    files.add(fs.open(fstat.getPath()));
                }
            }
        }
        return files;
    }

    @Override
    public int run(String[] args) throws Exception {
        options = new Options();
        CmdLineParser parser = new CmdLineParser(options);
        String s = options.toString();

        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
            return -1;
        }

        this.userIndex = HashBiMap.create();
        this.itemIndex = HashBiMap.create();

        // split into actions and store in subdirs
        // create indexes for users and items
        Path inputPath = new Path(options.getInputDir());
        FileSystem fs = inputPath.getFileSystem(new JobConf());
        Path outputPath = new Path(options.getOutputDir());
        // todo: can put this into m/r if it helps speed up
        split(inputPath, outputPath);// split into actions and store in subdirs

        Path indexesPath = new Path(options.getIndexDir());
        Path userIndexPath = new Path(options.getIndexDir(), options.getUserIndexFile());
        Path itemIndexPath = new Path(options.getIndexDir(), options.getItemIndexFile());
        if (fs.exists(userIndexPath)) fs.delete(userIndexPath, false);//delete file only!
        if (fs.exists(itemIndexPath)) fs.delete(itemIndexPath, false);//delete file only!
        // get the size of the matrices and put them where the calling job
        // can find them
        HadoopUtil.writeInt(getNumberOfUsers(), new Path(indexesPath, options.getNumUsersFile()), getConf());
        HadoopUtil.writeInt(getNumberOfItems(), new Path(indexesPath, options.getNumItemsFile()), getConf());
        //write the indexes to tsv files
        saveIndexes(indexesPath);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new ActionSplitterJob(), args);
    }

    public ActionSplitterJob.Options getOptions() {
        //for cases when another class want to know how ActionSplitter is configured and
        //is not accessing a static option default
        return options;
    }

    /*
     * This class implements an option parser. Used instead of the Mahout Abstract Job
     * because the options are numerous.
     */
    public class Options {
        private static final String DEFAULT_ACTION_1 = "purchase";
        private static final String DEFAULT_ACTION_2 = "view";
        private static final String DEFAULT_ACTION_OTHER = "other";

        private static final String DEFAULT_NUM_USERS_FILE = "num-users.bin";
        private static final String DEFAULT_NUM_ITEMS_FILE = "num-items.bin";
        private static final int DEFAULT_TIMESTAMP_COLUMN = -1;
        private static final int DEFAULT_ITEM_ID_COLUMN = 2;
        private static final int DEFAULT_ACTION_COLUMN = 1;
        private static final int DEFAULT_USER_ID_COLUMN = 0;
        private static final String TSV_DELIMITER = "\t";
        private static final String CSV_DELIMITER = ",";
        private static final String DEFAULT_INPUT_DELIMITER = TSV_DELIMITER;
        private static final String DEFAULT_OUTPUT_DELIMITER = TSV_DELIMITER;
        public static final String DEFAULT_INDEX_DIR = "id-indexes";//assumed to be a dir in output unless specified
        public static final String DEFAULT_USER_INDEX_FILENAME = "user-index";
        public static final String DEFAULT_ITEM_INDEX_FILENAME = "item-index";
        private static final String DEFAULT_TEMP_PATH = "tmp";
        private static final String DEFAULT_INPUT_FILE_PATTERN = "part";//default is a hadoop created part-xxxx file

        private String action1 = DEFAULT_ACTION_1;
        private String action2 = DEFAULT_ACTION_2;
        private String action1Dir;
        private String action2Dir;
        private String actionOtherDir;
        private String action1File;
        private String action2File;
        private String actionOtherFile;
        private String numUsersFile = DEFAULT_NUM_USERS_FILE;
        private String numItemsFile = DEFAULT_NUM_ITEMS_FILE;
        private int actionColumn = DEFAULT_ACTION_COLUMN;
        private int timestampColumn = DEFAULT_TIMESTAMP_COLUMN;
        private int itemIdColumn = DEFAULT_ITEM_ID_COLUMN;
        private int userIdColumn = DEFAULT_USER_ID_COLUMN;
        private String inputDelimiter = DEFAULT_INPUT_DELIMITER;
        private String outputDelimiter = DEFAULT_OUTPUT_DELIMITER;
        private String tempPath = DEFAULT_TEMP_PATH;
        private String inputFilePattern = DEFAULT_INPUT_FILE_PATTERN;
        private String indexDir = DEFAULT_INDEX_DIR;
        private String itemIndexFile = DEFAULT_ITEM_INDEX_FILENAME;
        private String userIndexFile = DEFAULT_USER_INDEX_FILENAME;

        // required options
        private String inputDir;
        private String outputDir;

        Options() {
            //setup default values before processing args
            this.action1Dir = toDirName(DEFAULT_ACTION_1);
            this.action1File = this.action1Dir + getTextFileExtension();
            this.action2Dir = toDirName(DEFAULT_ACTION_2);
            this.action2File = this.action2Dir + getTextFileExtension();
            this.actionOtherDir = toDirName(DEFAULT_ACTION_OTHER);
            this.actionOtherFile = this.actionOtherFile + getTextFileExtension();
        }


        @Option(name = "--input", usage = "Dir that will be searched recursively for files that have mixed actions. These will be split into the output dir.", required = true)
        public Options setInputDir(String primaryInputDir) {
            this.inputDir = primaryInputDir;
            return this;
        }

        @Option(name = "--output", usage = "Output directory for recs.", required = true)
        public Options setOutputDir(String outputDir) {
            this.outputDir = outputDir;
            return this;
        }

        @Option(name = "-ifp", aliases = {"--inputFilePattern"}, usage = "Search --input recusively for files with this string included in their name. Optional: default = 'part'", required = false)
        public void setInputFilePattern(String inputFilePattern) {
            this.inputFilePattern = inputFilePattern;
        }

        @Option(name = "--action1", aliases = {"-a1"}, usage = "String to id primary action. Optional: default = 'purchase'", required = false)
        public Options setAction1(String action1) {
            this.action1 = action1;
            this.action1Dir = toDirName(action1);
            this.action1File = this.action1Dir + getTextFileExtension();
            return this;
        }

        @Option(name = "--action2", aliases = {"-a2"}, usage = "String to id secondary action. Optional: default = 'view'", required = false)
        public Options setAction2(String action2) {
            this.action2 = action2;
            this.action2Dir = toDirName(action2);
            this.action2File = this.action2Dir + getTextFileExtension();
            return this;
        }

        @Option(name = "--timestampCol", usage = "Which column contains the timestamp. Optional: default = 0", required = false)
        public void setTimestampColumn(int timestampColumn) {
            this.timestampColumn = timestampColumn;
        }

        @Option(name = "--userIDCol", usage = "Which column contains the user Id. Optional: default = 4", required = false)
        public void setUserIdColumn(int userIdColumn) {
            this.userIdColumn = userIdColumn;
        }

        @Option(name = "--actionIDCol", usage = "Which column contains the action. Optional: default = 2", required = false)
        public Options setActionColumn(int actionColumn) {
            this.actionColumn = actionColumn;
            return this;
        }

       @Option(name = "--itemIDCol", usage = "Which column contains the action. Optional: default = 1", required = false)
        public void setItemIdColumn(int itemIdColumn) {
            this.itemIdColumn = itemIdColumn;
        }

        @Option(name = "--inputDelim", usage = "What string to use as column delimiter forinput. Optional: default = '/\t' = tab", required = false)
        public void setInputDelimiter(String inputDelimiter) {
            this.inputDelimiter = inputDelimiter;
        }

        @Option(name = "--outputDelim", usage = "What string to use as column delimiter in output. Optional: default = ',' = comma", required = false)
        public void setOutputDelimiter(String outputDelimiter) {
            this.outputDelimiter = outputDelimiter;
        }

        @Option(name = "--indexDir", usage = "Place for external to internal item and user ID indexes. This directory will be deleted before the indexes are written. Optional: defualt = 'id-indexes'", required = false)
        public Options setIndexDir(String indexDir) {
            this.indexDir = indexDir;
            return this;
        }

        private String toDirName(String action) {
            return action.toLowerCase().replace("_", "-").replace(" ", ".");
        }

        public String getAction1() {
            return action1;
        }

        private String getTextFileExtension() {
            return getOutputDelimiter().equals(CSV_DELIMITER) ? ".csv" : ".tsv";
        }

        public String getInputFilePattern() {
            return inputFilePattern;
        }

        public String getAction2() {
            return action2;
        }

        public int getActionColumn() {
            return actionColumn;
        }

        public int getTimestampColumn() {
            return timestampColumn;
        }

        public int getItemIdColumn() {
            return itemIdColumn;
        }

        public int getUserIdColumn() {
            return userIdColumn;
        }

        public String getInputDelimiter() {
            return inputDelimiter;
        }

        public String getOutputDelimiter() {
            return outputDelimiter;
        }

        public String getIndexDir() {
            return indexDir;
        }

        public String getInputDir() {
            return this.inputDir;
        }

        public String getOutputDir() {
            return this.outputDir;
        }

        public String getUserIndexFile() {
            return userIndexFile;
        }

        public String getItemIndexFile() {
            return itemIndexFile;
        }

        public String getAction1Dir() {
            return action1Dir;
        }

        public String getAction2Dir() {
            return action2Dir;
        }

        public String getActionOtherDir() {
            return actionOtherDir;
        }

        public String getAction1File() {
            return action1File;
        }

        public String getAction2File() {
            return action2File;
        }

        public String getActionOtherFile() {
            return actionOtherFile;
        }

        public String getNumUsersFile() {
            return numUsersFile;
        }

        public String getNumItemsFile() {
            return numItemsFile;
        }

        @Override
        public String toString() {
            String options = ReflectionToStringBuilder.toString(this, ToStringStyle.MULTI_LINE_STYLE);
            options = options.replaceAll("\n", "\n#");
            Date date = new Date();
            SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy h:mm:ss a");
            String formattedDate = sdf.format(date);
            options = options + "\n# Timestamp for data creation = " + formattedDate;
            return options = new StringBuffer(options).insert(0, "#").toString();
        }
    }
}
