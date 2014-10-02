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

/**
 * <p>Splits input file by actions, translates string IDs in the file into ints required by Mahout. Runs a distributed item-based recommender and cross-recommender job in the Mahout style. The concept behind this is based on the fact that when preferences are taken from user actions, it is often useful to use one action for recommendation but the other will also work if the secondary action co-occurs with the first. For example views are predictive of purchases if the viewed item was indeed purchased.</p>
 * <p>The job will execute the Mahout item-based recommender and store all recs for all users OR output the action1 similarity matrix to Solr for use in as an online recommender. Likewise it will calculate all cross-recommendations OR output the cross-similarity matrix to Solr.</p>
 * <p>A = matrix of views by user</p>
 * <p>B = matrix of purchases by user</p>
 * <p>[B'B]H_p = R_p, recommendations from purchase actions with strengths</p>
 * <p>[B'A]H_v = R_v, recommendations from view actions (where there was a purchase) with strengths</p>
 * <p>R_p + R_v = R, assuming a non-weighted linear combination</p>
 * <p>This job currently only calculates R_v since the usual RecommenderJob can be used to create R_p</p>
 * <p>Further row similarities of [B'A]' will give item similarities for views with purchases and these are calculated by this job</p>
 * <p>Preferences in the input file will be written to look like {@code userID, itemID, 1.0}</p>
 * <p>
 * The preference value is assumed to be parseable as a {@code double}. The user IDs and item IDs are
 * parsed as {@code long}s.
 * </p>
 * <p>Command line arguments for class are available by executing with no options. todo:put final options here.</p>
 * <p/>
 * <p>Note that because of how Hadoop parses arguments, all "-D" arguments must appear before all other
 * arguments.</p>
 */

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.mahout.cf.taste.hadoop.item.RecommenderJob;
import org.apache.mahout.cf.taste.hadoop.preparation.PreparePreferenceMatrixJob;
import org.apache.mahout.common.HadoopUtil;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public final class RecommenderUpdateJob extends Configured implements Tool {
    private static Logger LOGGER = Logger.getRootLogger();

    private int numberOfUsers;
    private int numberOfItems;
    private static Options options;

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


        cleanOutputDirs(options);
        Path prefFilesRootDir = new Path(options.getOutputDir());
        FileSystem fs = prefFilesRootDir.getFileSystem(getConf());
        Path indexesPath = new Path(prefFilesRootDir, options.getIndexesDir());
        Path prefsPath = new Path(prefFilesRootDir, options.getPrefsDir());
        options.setPrefsDir(prefsPath.toString());

        // split into actions and store in subdirs
        // create an index/dictionary for users and items
        // this job cleans out the output dir first
        ActionSplitterJob aj = new ActionSplitterJob();
        ToolRunner.run(getConf(), aj, new String[]{
            "--input", options.getInputDir(),
            "--output", prefsPath.toString(),
            "--indexDir", indexesPath.toString(),
            "--inputFilePattern", options.getFileNamePatternString(),
            "--action1", options.getAction1(),
            "--action2", options.getAction2(),
            "--inputDelim", options.getInputDelim(),
            "--outputDelim", options.getOutputDelim(),
            "--actionIDCol", Integer.toString(options.getActionColumn()),
            "--itemIDCol", Integer.toString(options.getItemIDColumn()),
            "--userIDCol", Integer.toString(options.getUserIDColumn()),
        });

        // need to get the number of users and items from the splitter, which also creates indexes
        this.numberOfUsers = HadoopUtil.readInt(new Path(indexesPath, aj.getOptions().getNumUsersFile()), getConf());
        this.numberOfItems = HadoopUtil.readInt(new Path(indexesPath, aj.getOptions().getNumItemsFile()), getConf());
        // these are single value binary files written with
        // HadoopUtil.writeInt(this.numberOfUsers, getOutputPath(NUM_USERS), getConf());

        options.setInputDir(prefFilesRootDir.toString());

        String action1PrefsPath = new Path(new Path(options.getPrefsDir()), aj.getOptions().getAction1Dir()).toString();
        String action2PrefsPath = new Path(new Path(options.getPrefsDir()), aj.getOptions().getAction2Dir()).toString();

        //LOGGER.info("prefFilesRootDir.toString() = "+prefFilesRootDir.toString());
        //LOGGER.info("options.getPrefsDir() = "+options.getPrefsDir());
        //LOGGER.info("aj.getOptions().getAction1Dir() = "+aj.getOptions().getAction1Dir());
        //LOGGER.info("action1PrefsPath = "+action1PrefsPath.toString());
        //LOGGER.info("action2PrefsPath = "+action2PrefsPath.toString());
        ToolRunner.run(getConf(), new RecommenderJob(), new String[]{
            "--input", action1PrefsPath,
            "--output", options.getPrimaryRecsPath(),
            "--similarityClassname", options.getSimilairtyType(),
            //need the seqfile for the similarity matrix even if output to Solr, this job puts it in the temp dir.
            "--tempDir", options.getPrimaryTempDir(),
            "--sequencefileOutput"
        });
        //Now move the similarity matrix to the p-recs/sims location rather than leaving is in the tmp dir
        //this will be written to Solr if specified in the options.

        if (options.getDoXRecommender()) {
            //note: similairty class is not used, cooccurrence only for now
            ToolRunner.run(getConf(), new XRecommenderJob(), new String[]{
                "--input", options.getAllActionsDir(),
                "--output", options.getSecondaryOutputDir(),
                "--similarityClassname", "SIMILARITY_LOGLIKELIHOOD",
                "--outputPathForSimilarityMatrix", options.getSecondarySimilarityMatrixPath(),
                "--tempDir", options.getSecondaryTempDir(),
                "--numUsers", Integer.toString(this.numberOfUsers),
                "--numItems", Integer.toString(this.numberOfItems),
                "--primaryPrefs", action1PrefsPath,
                "--secondaryPrefs", action2PrefsPath,
            });
        }

        Path bBSimilarityMatrixDRM = new Path(options.getPrimarySimilarityMatrixPath());
        Path bASimilarityMatrixDRM = new Path(options.getSecondarySimilarityMatrixPath());
        Path primaryActionDRM = new Path(new Path(options.getPrimaryTempDir(), RecommenderJob.DEFAULT_PREPARE_PATH), PreparePreferenceMatrixJob.USER_VECTORS);
        Path secondaryActionDRM = new Path(new Path(options.getSecondaryTempDir(), XRecommenderJob.DEFAULT_PREPARE_DIR), PrepareActionMatricesJob.USER_VECTORS_A);

        if(options.getDoXRecommender()){
            //Next step is to take the history and similarity matrices, join them by id and write to solr docs
            LOGGER.info(
                "\n===========\n\n\n"+
                "  About to call WriteToSolr with cross-recommendations:\n"+
                    "    B matrix path: "+primaryActionDRM.toString()+"\n"+
                    "    A matrix path: "+secondaryActionDRM.toString()+"\n"+
                    "    [B'B] matrix path: "+bBSimilarityMatrixDRM.toString()+"\n"+
                    "    [B'A] matrix path: "+bASimilarityMatrixDRM.toString()+"\n"+
                    "    Output path: "+options.getOutputDir()+"\n"+
                "\n\n===========\n"
            );
            ToolRunner.run(getConf(), new WriteToSolrJob(), new String[]{
                "--itemCrossSimilarityMatrixDir", bASimilarityMatrixDRM.toString(),
                "--indexDir", indexesPath.toString(),
                "--itemSimilarityMatrixDir", bBSimilarityMatrixDRM.toString(),
                "--usersPrimaryHistoryDir", primaryActionDRM.toString(),
                "--usersSecondaryHistoryDir", secondaryActionDRM.toString(),
                "--output", options.getOutputDir(),
            });
        } else {
            LOGGER.info(
                "\n===========\n\n\n"+
                    "  About to call WriteToSolr with single actions recommendations:\n"+
                    "    B matrix path: "+primaryActionDRM.toString()+"\n"+
                    "    [B'B] matrix path: "+bBSimilarityMatrixDRM.toString()+"\n"+
                    "    Output path: "+options.getOutputDir()+"\n"+
                    "\n\n===========\n"
            );
            ToolRunner.run(getConf(), new WriteToSolrJob(), new String[]{
                "--indexDir", indexesPath.toString(),
                "--itemSimilarityMatrixDir", bBSimilarityMatrixDRM.toString(),
                "--usersPrimaryHistoryDir", primaryActionDRM.toString(),
                "--output", options.getOutputDir(),
            });
        }

        /*
        ToolRunner.run(getConf(), new WriteToSolrJob(), new String[]{
            "--itemCrossSimilarityMatrixDir", "../out/s-recs/sims",
            "--indexDir", "../out/id-indexes",
            "--itemSimilarityMatrixDir", "../out/p-recs/sims",
            "--usersPrimaryHistoryDir", "../out/actions/p-action",
            "--usersSecondaryHistoryDir", "../out/actions/s-action",
            "--output", "../out",
        });
        */

        //move user history and similarity matrices
        //move stuff out of temp for now, may not need all these
        moveMatrices();

        return 0;
    }

    private static void cleanOutputDirs(Options options) throws IOException {
        FileSystem fs = FileSystem.get(new JobConf());
        //instead of deleting all, delete only the ones we overwrite
        Path primaryOutputDir = new Path(options.getPrimaryOutputDir());
        try {
            fs.delete(primaryOutputDir, true);
        } catch (Exception e) {
            LOGGER.info("No primary output dir to delete, skipping.");
        }
        Path secondaryOutputDir = new Path(options.getSecondaryOutputDir());
        try {
            fs.delete(secondaryOutputDir, true);
        } catch (Exception e) {
            LOGGER.info("No secondary output dir to delete, skipping.");
        }
        try {
            fs.delete(new Path(options.getPrimaryRecsPath()), true);
        } catch (Exception e) {
            LOGGER.info("No recs dir to delete, skipping.");
        }
        try {
            fs.delete(new Path(options.getTempDir()), true);
        } catch (Exception e) {
            LOGGER.info("No temp dir to delete, skipping.");
        }
        try {
            fs.delete(new Path(options.getOutputDir(), options.getPrimaryActionHistoryDir()), true);
        } catch (Exception e) {
            LOGGER.info("No action1 history dir to delete, skipping.");
        }
        if(options.getDoXRecommender()){
            try {
                fs.delete(new Path(options.getOutputDir(), options.getSecondaryActionHistoryDir()), true);
            } catch (Exception e) {
                LOGGER.info("No action2 history dir to delete, skipping.");
            }
        }
    }


    private void moveMatrices() throws IOException {
        //so it can output to Solr if options specify
        FileSystem fs = FileSystem.get(getConf());
        Path from = new Path(options.getPrimarySimilarityMatrixPath());
        Path to = new Path(options.getPrimaryOutputDir(), XRecommenderJob.SIMS_MATRIX_DIR);//steal the dir name from Xrec
        fs.rename(from, to);
        //move the primary user action matrix to output
        from = new Path(new Path(options.getPrimaryTempDir(), RecommenderJob.DEFAULT_PREPARE_PATH), PreparePreferenceMatrixJob.USER_VECTORS);
        to = new Path(options.getOutputDir(), options.getPrimaryActionHistoryDir());
        fs.rename(from, to);
        //if it was created move the secondary user action matrix to output
        if(options.getDoXRecommender()){
            from = new Path(new Path(options.getSecondaryTempDir(), XRecommenderJob.DEFAULT_PREPARE_DIR), PrepareActionMatricesJob.USER_VECTORS_A);
            to = new Path(options.getOutputDir(), options.getSecondaryActionHistoryDir());
            fs.rename(from, to);
        }
    }


    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new RecommenderUpdateJob(), args);
    }

    // Command line options for this job. Execute the main method above with no parameters
    // to get a help listing.
    //

    public class Options {

        private String similairtyType = LOG_LIKELIHOOD;//hard coded to log-likelihood
        public static final String LOG_LIKELIHOOD = "SIMILARITY_LOGLIKELIHOOD";
        public static final String EUCLIDEAN = "SIMILARITY_EUCLIDEAN";
        public static final String COSINE = "SIMILARITY_COSINE";
        public static final String TANIMOTO = "SIMILARITY_TANIMOTO_COEFFICIENT";
        public static final String CITY_BLOCK = "SIMILARITY_CITY_BLOCK";

        private static final int DEFAULT_TIMESTAMP_COLUMN = -1;//not used by default
        private static final int DEFAULT_UESERID_COLUMN = 0;//not used by default
        private static final int DEFAULT_ACTION_COLUMN = 1;//not used by default
        private static final int DEFAULT_ITEMID_COLUMN = 2;//not used by default
        //default TSV preferences layout = userID   action  itemID
        private static final String DEFAULT_ACTION_1 = "purchase";
        private static final String DEFAULT_ACTION_2 = "view";
        private static final String DEFAULT_ACTION_3 = "";// not used but can be split out if specified
        private static final String PRIMARY_OUTPUT_DIR = "p-recs";
        private static final String SECONDARY_OUTPUT_DIR = "s-recs";
        private static final String DEFAULT_PREFS_DIR = "prefs";
        private static final String DEFAULT_INDEXES_DIR = "id-indexes";
        private static final String DEFAULT_ACTION_HISTORY_DIR = "actions";
        private static final String DEFAULT_PRIMARY_ACTION_HISTORY_DIR = "p-action";
        private static final String DEFAULT_SECONDARY_ACTION_HISTORY_DIR = "s-action";
        private static final String DEFAULT_TEMP_DIR = "tmp";
        private static final String PRIMARY_TEMP_DIR = "tmp1";
        private static final String ROOT_RECS_DIR = "recs";
        private static final String SECONDARY_TEMP_DIR = "tmp2";
        private static final String PRIMARY_SIMILARITY_MATRIX = "similarityMatrix";//defined as a quoted String in mahout
        public static final String ROOT_SIMS_DIR = "sims";//defined as a quoted String in mahout
        private static final String DEFAULT_FILE_PATTERN = ".*tsv";

        private static final String DEFAULT_DELIMITER = "\t";

        private int timestampColumn = DEFAULT_TIMESTAMP_COLUMN;
        private int userIDColumn = DEFAULT_UESERID_COLUMN;
        private int actionColumn = DEFAULT_ACTION_COLUMN;
        private int itemIDColumn = DEFAULT_ITEMID_COLUMN;
        private String action1 = DEFAULT_ACTION_1;
        private String action2 = DEFAULT_ACTION_2;
        private String action3 = DEFAULT_ACTION_3;
        private int numberOfRecsPerUser = 10;
        private String inputDir;//required
        private String outputDir;//required
        private String tempDir = DEFAULT_TEMP_DIR;
        private String prefsDir = DEFAULT_PREFS_DIR;
        private Boolean doXRecommender = false;
        private String fileNamePatternString = DEFAULT_FILE_PATTERN;
        private String indexesDir = DEFAULT_INDEXES_DIR;
        private String primaryActionHistoryDir;
        private String secondaryActionHistoryDir;
        private String inputDelimiter = DEFAULT_DELIMITER;
        private String outputDelimiter = DEFAULT_DELIMITER;


        Options() {
            //these are relative to the output path
            this.primaryActionHistoryDir = new Path(DEFAULT_ACTION_HISTORY_DIR, DEFAULT_PRIMARY_ACTION_HISTORY_DIR).toString();
            this.secondaryActionHistoryDir = new Path(DEFAULT_ACTION_HISTORY_DIR, DEFAULT_SECONDARY_ACTION_HISTORY_DIR).toString();
        }

        @Option(name = "-i", aliases = {"--input"}, usage = "Input directory searched recursively for files in 'ExternalID' format where ID are unique strings and preference files contain combined actions with action IDs. Subdirs will be created by action type, so 'purchase', 'view', etc.", required = true)
        public void setInputDir(String primaryInputDir) {
            this.inputDir = primaryInputDir;
        }

        @Option(name = "-o", aliases = {"--output"}, usage = "Output directory for recs. There will be two subdirs one for the primary recommender and one for the secondry/cross-recommender each of which will have item similarities and user history recs.", required = true)
        public void setOutputDir(String outputDir) {
            this.outputDir = outputDir;
        }

        @Option(name = "-uidc", aliases = {"--userIDColumn"}, usage = "Which column has the userID (optional). Default: 0", required = false)
        public void setUserIDColumn(int userIDColumn) {
            this.userIDColumn = userIDColumn;
        }

        @Option(name = "-ac", aliases = {"--actionColumn"}, usage = "Which column has the action (optional). Default: 1", required = false)
        public void setActionColumn(int actionColumn) {
            this.actionColumn = actionColumn;
        }

        @Option(name = "-iidc", aliases = {"--itemIDColumn"}, usage = "Which column has the itemID (optional). Default: 2", required = false)
        public void setItemIDColumn(int itemIDColumn) {
            this.itemIDColumn = itemIDColumn;
        }

        @Option(name = "-a1", aliases = {"--action1"}, usage = "String respresenting action1, the primary preference action (optional). Default: 'purchase'", required = false)
        public void setAction1(String action1) {
            this.action1 = action1;
        }

        @Option(name = "-a2", aliases = {"--action2"}, usage = "String respresenting action2, the secondary preference action (optional). Default: 'view'", required = false)
        public void setAction2(String action2) {
            this.action2 = action2;
        }

        @Option(name = "-ifp", aliases = {"--inputFilePattern"}, usage = "Match this regex pattern when searching for action log files, must match entire file name with the regex (optional). Default: '.*tsv'. Can be ignored if specifying a single file with --input.", required = false)
        public void setFileNamePatternString(String fileNamePatternString) {
            this.fileNamePatternString = fileNamePatternString;
        }

        @Option(name = "-x", aliases = {"--xRecommend"}, usage = "Create cross-recommender for multiple actions (optional). Default: false.", required = false)
        public void setDoXRecommender(Boolean doXRecommender) {
            this.doXRecommender = doXRecommender;
        }

        @Option(name = "-ix", aliases = {"--indexDir"}, usage = "Where to put user and item indexes (optional). Default: 'id-indexes'", required = false)
        public void setIndexesDir(String indexesDir) {
            this.indexesDir = indexesDir;
        }

        @Option(name = "-t", aliases = {"--tempDir"}, usage = "Place for intermediate data. Things left after the jobs but erased before starting new ones.", required = false)
        public void setTempDir(String tempDir) {
            this.tempDir = tempDir;
        }

        @Option(name = "-r", aliases = {"--recsPerUser"}, usage = "Number of recommendations to return for each request. Default = 10.", required = false)
        public void setNumberOfRecsPerUser(int numberOfRecsPerUser) {
            this.numberOfRecsPerUser = numberOfRecsPerUser;
        }

        @Option(name = "-s", aliases = {"--similarityType"}, usage = "Similarity measure to use. Default SIMILARITY_LOGLIKELIHOOD. Note: this is only used for primary recs and secondary item similarities.", required = false)
        public void setSimilairtyType(String similairtyType) {
            this.similairtyType = similairtyType;
        }

        @Option(name = "-id", aliases = {"--inputDelim"}, usage = "Assigned delimiter of input file", required = false)
        public void setInputDelim(String delim) {
            this.inputDelimiter = delim;
        }

        @Option(name = "-od", aliases = {"--outputDelim"}, usage = "Assigned delimiter of output file", required = false)
        public void setOutputDelim(String delim) {
            this.outputDelimiter = delim;
        }

        public String getPrimaryActionHistoryDir() {
            return primaryActionHistoryDir;
        }

        public String getSecondaryActionHistoryDir() {
            return secondaryActionHistoryDir;
        }

        public int getNumberOfRecsPerUser() {
            return this.numberOfRecsPerUser;
        }

        public String getSimilairtyType() {
            return similairtyType;
        }

        public String getOutputDir() {
            return this.outputDir;
        }

        public String getPrimaryOutputDir() {
            return new Path(this.outputDir, PRIMARY_OUTPUT_DIR).toString();
        }

        public String getSecondaryOutputDir() {
            return new Path(this.outputDir, SECONDARY_OUTPUT_DIR).toString();
        }

        public String getInputDir() {
            return this.inputDir;
        }

        public String getTempDir() {
            return this.tempDir;
        }

        public Boolean getDoXRecommender() {
            return doXRecommender;
        }

        private String getPrimaryRecsPath() {
            return new Path(getPrimaryOutputDir(), ROOT_RECS_DIR).toString();
        }

        private String getAllActionsDir() {
            return new Path(getInputDir(), getPrefsDir()).toString();
        }

        private String getPrimaryTempDir() {
            return new Path(getTempDir(), PRIMARY_TEMP_DIR).toString();
        }

        private String getSecondaryTempDir() {
            return new Path(getTempDir(), SECONDARY_TEMP_DIR).toString();
        }

        public String getPrefsDir() {
            return prefsDir;
        }

        public void setPrefsDir(String prefsDir) {
            this.prefsDir = prefsDir;
        }

        public String getFileNamePatternString() {
            return fileNamePatternString;
        }

        public String getIndexesDir() {
            return indexesDir;
        }

        public String getAction2() {
            return action2;
        }

        public String getAction1() {
            return action1;
        }

        public int getItemIDColumn() {
            return itemIDColumn;
        }

        public int getActionColumn() {
            return actionColumn;
        }

        public int getTimestampColumn() {
            return timestampColumn;
        }

        public String getInputDelim() {
            return inputDelimiter; 
        }

        public String getOutputDelim() {
            return inputDelimiter; 
        }

        //todo: ignored so this is a stub. May wish to order perfs by timestamp before downsampling/truncating history
        public void setTimestampColumn(int timestampColumn) {
            this.timestampColumn = timestampColumn;
        }

        public int getUserIDColumn() {
            return userIDColumn;
        }

        public String getPrimarySimilarityMatrixPath() {
            return new Path(getPrimaryTempDir(), PRIMARY_SIMILARITY_MATRIX).toString();
        }

        public String getSecondarySimilarityMatrixPath() {
            return new Path(getSecondaryOutputDir(), ROOT_SIMS_DIR).toString();
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
