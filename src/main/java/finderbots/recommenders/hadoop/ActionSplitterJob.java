package finderbots.recommenders.hadoop;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.mahout.common.AbstractJob;
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
import java.util.Map;

/**
 * User: pat
 * Date: 4/9/13
 * Time: 8:05 AM
 * Takes a directory of files named 'ext-id.txt' or whatever is specified in the options
 * and splits them by action so they can
 * be fed to the recommender training jobs. This is done with HDFS text files as input and creates
 * the same as output.
 *
 * This job creates a single item and user ID space by accumulating all IDs into two BiMaps. This
 * can scale only so far and is single threaded.
 *
 * Danger: This job overwrites anything in the output directories by default, you will not be warned.
 *
 * Todo: create cascading jobs to mapreduce this operation. The trick for a simple job is to
 *    put the indexes in memory and make them available to the m/r jobs so they can be added to
 *    then write them out as the last phase of the job.
 *
 * Note: This does not strictly need to be derived from AbstractJob but is so it can be rewritten
 * to be mapreduce and to fit into Mahout.
 */

public class ActionSplitterJob extends AbstractJob {
    private static Logger LOGGER = Logger.getRootLogger();

    private BiMap<String, String> userIndex;
    private BiMap<String, String> itemIndex;
    private static Options options;


    public void split(Path baseInputDir, Path baseOutputDir) throws IOException {
        FileSystem fs = baseInputDir.getFileSystem(getConf());
        Path action1DirPath = new Path(baseOutputDir, options.getAction1Dir());
        Path action2DirPath = new Path(baseOutputDir, options.getAction2Dir());
        Path action3DirPath = new Path(baseOutputDir, options.getAction3Dir());
        Path actionOtherDirPath = new Path(baseOutputDir, options.getActionOtherDir());
        Path action1FilePath = new Path(action1DirPath, options.getAction1File());
        Path action2FilePath = new Path(action2DirPath, options.getAction2File());
        Path action3FilePath = new Path(action3DirPath, options.getAction3File());
        Path actionOtherFilePath = new Path(actionOtherDirPath, options.getActionOtherFile());
        FSDataOutputStream action1File;
        FSDataOutputStream action2File;
        FSDataOutputStream action3File;
        FSDataOutputStream actionOtherFile;

        //Eval output in case we use them for MAP
        //1357152044224	some-user-id-string	action-string	item-id-string
        //the eval files are written using external IDs so they can be used as queries
        //for an offline evaluator. Other split files are written using Mahout internal IDs (ints)
        FSDataOutputStream evalAction1File;//primary action with external ids, used for eval precision calc
        FSDataOutputStream evalAction3File;//primary action with external ids, used for queries in eval
        if (!fs.exists(baseOutputDir)) {
            LOGGER.info("Preference output dir:"+baseOutputDir.toString()+" does not exist. creating it.");
            fs.mkdirs(baseOutputDir);
        }

        if(fs.exists(action1DirPath)) fs.delete(action1DirPath, true);
        if(fs.exists(action2DirPath)) fs.delete(action2DirPath, true);
        if(fs.exists(action3DirPath)) fs.delete(action3DirPath, true);
        if(fs.exists(actionOtherDirPath)) fs.delete(actionOtherDirPath, true);

        // cleaned out prefs if they existed, now create a place to put the new ones
        fs.mkdirs(action1DirPath);
        fs.mkdirs(action2DirPath);
        fs.mkdirs(action3DirPath);
        fs.mkdirs(actionOtherDirPath);
        action1File = fs.create(action1FilePath);
        action2File = fs.create(action2FilePath);
        action3File = fs.create(action3FilePath);
        actionOtherFile = fs.create(actionOtherFilePath);

        List<FSDataInputStream> actionFiles = getActionFiles(baseInputDir);

        Integer uniqueUserIDCounter = 0;
        Integer uniqueItemIDCounter = 0;
        for (FSDataInputStream stream : actionFiles) {
            BufferedReader bin = new BufferedReader(new InputStreamReader(stream));
            String actionLogLine;
            while ((actionLogLine = bin.readLine()) != null) {//get user to make a rec for
                String[] columns = actionLogLine.split(options.getInputDelimiter());
                String timestamp = columns[options.getTimestampColumn()].trim();
                String externalUserIDString = columns[options.getUserIdColumn()].trim();
                String externalItemIDString = columns[options.getItemIdColumn()].trim();
                String actionString = columns[options.getActionColumn()].trim();

                // create a bi-directional index of enternal->internal ids
                String internalUserID;
                String internalItemID;
                if (this.userIndex.containsKey(externalUserIDString)) {// already in the index
                    internalUserID = this.userIndex.get(externalUserIDString);
                } else {
                    internalUserID = uniqueUserIDCounter.toString();
                    this.userIndex.forcePut(externalUserIDString, internalUserID);
                    uniqueUserIDCounter += 1;
                    if(uniqueUserIDCounter % 10000 == 0) LOGGER.debug("Splitter processed: "+Integer.toString(uniqueUserIDCounter)+" unique users.");
                }
                if (this.itemIndex.containsKey(externalItemIDString)) {// already in the index
                    internalItemID = this.itemIndex.get(externalItemIDString);
                } else {
                    internalItemID = uniqueItemIDCounter.toString();
                    this.itemIndex.forcePut(externalItemIDString, internalItemID);
                    uniqueItemIDCounter += 1;
                }
                if(actionString.equals(options.getAction1())){
                    action1File.writeBytes(internalUserID + options.getOutputDelimiter() + internalItemID + options.getOutputDelimiter() + "1.0\n");
                } else if(actionString.equals(options.getAction2())){
                     action2File.writeBytes(internalUserID + options.getOutputDelimiter() + internalItemID + options.getOutputDelimiter() + "1.0\n");
                } else if(actionString.equals(options.getAction3())){
                    action3File.writeBytes(internalUserID + options.getOutputDelimiter() + internalItemID + options.getOutputDelimiter() + "1.0\n");
                } else {
                    actionOtherFile.writeBytes(actionLogLine);//write what's not recognized
                    break;
                }
            }
        }
        action1File.close();
        action2File.close();
        action3File.close();
        actionOtherFile.close();
        int i = 0;
    }

    public void saveIndexes(Path where) throws IOException {
        Path userIndexPath = new Path(where, options.getUserIndexFile());
        Path itemIndexPath = new Path(where, options.getItemIndexFile());
        FileSystem fs = where.getFileSystem(new JobConf());
        if (fs.getFileStatus(where).isDir()) {
            FSDataOutputStream userIndexFile = fs.create(userIndexPath);
            this.saveIndex(userIndex, userIndexFile);
            FSDataOutputStream itemIndexFile = fs.create(itemIndexPath);
            this.saveIndex(itemIndex, itemIndexFile);
        } else {
            throw new IOException("Bad locaton for ID Indexes: " + where.toString());
        }
    }

    public void saveIndex(BiMap<String, String> map, FSDataOutputStream file) throws IOException {

        for (Map.Entry e : map.entrySet()) {
            file.writeBytes(e.getKey().toString() + options.getOutputDelimiter() + e.getValue().toString() + "\n");
        }
        file.close();
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
                if(fstat.isDir()){
                    files.addAll(getActionFiles(fstat.getPath()));
                } else //todo: this should look for a file name or pattern, not only a hadoop part-xxxx file
                    if(
                        (fstat.getPath().getName().contains("art-") && !fstat.isDir()) ||
                        (!fstat.getPath().getName().startsWith("_") && !fstat.getPath().getName().startsWith("."))
                    ){
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
        // get the size of the matrixes and put them where the calling job
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

    // Command line options for this job. Execute the main method above with no parameters
    // to get a help listing.
    //

    //These first few are available outside the Job statically from the class
    //make sure the options to the job have been parsed before these are queried
    //Todo: check for this error and warn about it otherwise a null pointer exception will be thrown?

    public static String getNumberOfUsersFile(){
        return options.getNumUsersFile();
    }

    public static String getNumberOfItemsFile(){
        return options.getNumItemsFile();
    }

    public static String getAction1Dir(){
        return options.getAction1Dir();
    }

    public static String getAction2Dir(){
        return options.getAction2Dir();
    }

    public static String getAction3Dir(){
        return options.getAction3Dir();
    }

    public ActionSplitterJob.Options getOptions(){
        return options;
    }

   /*
    * This class implements an option parser. It clashes somewhat with Mahouts in AbstractJob since it
    * does some of the same things in a slightly different way. Used because the options are so numerous
    * the AbstractJob functionality is not as nicely self-documenting.
    */
    public class Options {
        //action 1 derived
        private static final String DEFAULT_ACTION_1 = "purchase";
        private static final String DEFAULT_ACTION_2 = "add-to-cart";
        private static final String DEFAULT_ACTION_3 = "view";
        private static final String DEFAULT_ACTION_OTHER = "other";

        private static final String DEFAULT_ACTION_EVAL_DIR = "eval";
        private static final String DEFAULT_NUM_USERS_FILE = "num-users.bin";
        private static final String DEFAULT_NUM_ITEMS_FILE = "num-items.bin";
        private static final int DEFAULT_TIMESTAMP_COLUMN = 0;
        private static final int DEFAULT_ITEM_ID_COLUMN = 1;
        private static final int DEFAULT_ACTION_COLUMN = 2;
        private static final int DEFAULT_USER_ID_COLUMN = 4;
        private static final String TSV_DELIMETER = "\t";
        private static final String CSV_DELIMETER = ",";
        private static final String DEFAULT_INPUT_DELIMITER = TSV_DELIMETER;
        private static final String DEFAULT_OUTPUT_DELIMITER = TSV_DELIMETER;
        private static final String DEFAULT_INDEX_DIR_PATH = "id-indexes";
        private static final String DEFAULT_USER_INDEX_FILE = "user-index";
        private static final String DEFAULT_ITEM_INDEX_FILE = "item-index";
        private static final String DEFAULT_TEMP_PATH = "tmp";

        // these could be options if the defaults are not enough
        private String action1 = DEFAULT_ACTION_1;
        private String action2 = DEFAULT_ACTION_2;
        private String action3 = DEFAULT_ACTION_3;
        private String action1Dir;
        private String action2Dir;
        private String action3Dir;
        private String actionOtherDir;
        private String action1File;
        private String action2File;
        private String action3File;
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

        //optional or derived from required options
        private String indexDir = DEFAULT_INDEX_DIR_PATH;
        private String itemIndexFile = DEFAULT_ITEM_INDEX_FILE;
        private String userIndexFile = DEFAULT_USER_INDEX_FILE;

        // required options
        private String inputDir;
        private String outputDir;

        Options() {
            //setup default values before processing args
            this.action1Dir = toDirName(DEFAULT_ACTION_1);
            this.action1File = this.action1Dir + getTextFileExtension();
            this.action2Dir = toDirName(DEFAULT_ACTION_2);
            this.action2File = this.action2Dir + getTextFileExtension();
            this.action3Dir = toDirName(DEFAULT_ACTION_3);
            this.action3File = this.action3Dir + getTextFileExtension();
            this.actionOtherDir = toDirName(DEFAULT_ACTION_OTHER);
            this.actionOtherFile = this.actionOtherFile + getTextFileExtension();
        }


        public String getAction1() {
            return action1;
        }

        private String toDirName( String action ){
            return action.toLowerCase().replace("_", "-").replace(" ", ".");
        }

        private String getTextFileExtension(){
            return getOutputDelimiter().equals(CSV_DELIMETER) ? ".csv" : ".tsv";
        }

        @Option(name = "--action1", usage = "String to id primary action. Optional: default = 'purchase'", required = false)
        public Options setAction1(String action1) {
            this.action1 = action1;
            this.action1Dir = toDirName(action1);
            this.action1File = this.action1Dir + getTextFileExtension();
            return this;
        }

        public String getAction2() {
            return action2;
        }

        @Option(name = "--action2", usage = "String to id secondary action. Optional: default = 'add-to-cart'", required = false)
        public Options setAction2(String action2) {
            this.action2 = action2;
            this.action2Dir = toDirName(action2);
            this.action2File = this.action2Dir + getTextFileExtension();
            return this;
        }

        public String getAction3() {
            return action3;
        }

        @Option(name = "--action3", usage = "String to id tertiary action. Optional: default = 'view'", required = false)
        public Options setAction3(String action3) {
            this.action3 = action3;
            this.action3Dir = toDirName(action3);
            this.action3File = this.action3Dir + getTextFileExtension();
            return this;
        }

        public int getActionColumn() {
            return actionColumn;
        }

        @Option(name = "--actionCol", usage = "Which column contains the action. Optional: default = 2", required = false)
        public Options setActionColumn(int actionColumn) {
            this.actionColumn = actionColumn;
            return this;
        }

        public int getTimestampColumn() {
            return timestampColumn;
        }

        @Option(name = "--timestampCol", usage = "Which column contains the timestamp. Optional: default = 0", required = false)
        public void setTimestampColumn(int timestampColumn) {
            this.timestampColumn = timestampColumn;
        }

        public int getItemIdColumn() {
            return itemIdColumn;
        }

        @Option(name = "--itemCol", usage = "Which column contains the action. Optional: default = 1", required = false)
        public void setItemIdColumn(int itemIdColumn) {
            this.itemIdColumn = itemIdColumn;
        }

        public int getUserIdColumn() {
            return userIdColumn;
        }

        @Option(name = "--userCol", usage = "Which column contains the user Id. Optional: default = 4", required = false)
        public void setUserIdColumn(int userIdColumn) {
            this.userIdColumn = userIdColumn;
        }

        public String getInputDelimiter() {
            return inputDelimiter;
        }

        @Option(name = "--inputDelim", usage = "What string to use as column delimiter forinput. Optional: default = '/\t' = tab", required = false)
        public void setInputDelimiter(String inputDelimiter) {
            this.inputDelimiter = inputDelimiter;
        }

        public String getOutputDelimiter() {
            return outputDelimiter;
        }

        @Option(name = "--outputDelim", usage = "What string to use as column delimiter in output. Optional: default = ',' = comma", required = false)
        public void setOutputDelimiter(String outputDelimiter) {
            this.outputDelimiter = outputDelimiter;
        }

        public String getIndexDir() {
            return indexDir;
        }

        @Option(name = "--indexes", usage = "Place for external to internal item and user ID indexes. This directory will be deleted before the indexes are written. Optional: defualt = output-dir/id-indexes", required = false)
        public Options setIndexDir(String indexDir) {
            this.indexDir = indexDir;
            return this;
        }

        public String getInputDir() {
            return this.inputDir;
        }

        @Option(name = "--input", usage = "Dir that will be searched recursively for files that have mixed actions. These will be split into the output dir.", required = true)
        public Options setInputDir(String primaryInputDir) {
            this.inputDir = primaryInputDir;
            return this;
        }

        public String getOutputDir() {
            return this.outputDir;
        }

        @Option(name = "--output", usage = "Output directory for recs.", required = true)
        public Options setOutputDir(String outputDirPath) {
            this.outputDir = outputDir;
            return this;
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

        public String getAction3Dir() {
            return action3Dir;
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

        public String getAction3File() {
            return action3File;
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
