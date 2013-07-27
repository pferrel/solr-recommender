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
 * User: pat
 * Date: 4/2/13
 * Time: 8:49 AM
 */

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.HadoopUtil;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public final class RecommenderDriverJob extends AbstractJob {
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
        // If using externalid files treat input as root dir of externalid files
        Path prefFilesRootDir = new Path(options.getOutputDir());
        FileSystem fs = prefFilesRootDir.getFileSystem(getConf());
        Path indexesPath = new Path(prefFilesRootDir, options.getIndexesDir());
        Path prefsPath = new Path(prefFilesRootDir, options.getPrefsDir());
        options.setPrefsPath(prefsPath.toString());

        // split into actions and store in subdirs
        // create and index for users and another for items
        // this job cleans out the output dir first
        ActionSplitterJob aj = new ActionSplitterJob();
        ToolRunner.run(getConf(), aj, new String[]{
            "--inputDir", options.getInputDir(),
            "--outputDir", prefsPath.toString(),
            "--indexesDir", indexesPath.toString(),
            "--inputFilePattern", ".tsv",
        });

        // need to get the number of users and items from the splitter, which also creates indexes
        this.numberOfUsers = HadoopUtil.readInt(new Path(indexesPath, aj.getOptions().getNumUsersFile()), getConf());
        this.numberOfItems = HadoopUtil.readInt(new Path(indexesPath, aj.getOptions().getNumItemsFile()), getConf());
        // these are single value binary files written with
        // HadoopUtil.writeInt(this.numberOfUsers, getOutputPath(NUM_USERS), getConf());

        options.setInputDirPath(prefFilesRootDir.toString());

        //Path action1Prefs = new Path(new Path(getInputDir(),getPrefsDir()), ActionFileSplitterJob.ACTION_1_DIR).toString();
        String action1PrefsPath = new Path(new Path(options.getInputDir(),options.getPrefsDir()), aj.getOptions().getAction1Dir()).toString();

        ToolRunner.run(getConf(), new RecommenderJob(), new String[]{
            "--input", action1PrefsPath,
            "--output", options.getPrimaryRecsPath(),
            "--similarityClassname", options.getSimilairtyType(),
            //this options creates a text file from the similarity matrix--not needed if you want to use the sequencefile
            //which is already in the temp dir.
//            "--outputPathForSimilarityMatrix", options.getPrimarySimilarityMatrixPath(),
            "--tempDir", options.getPrimaryTempDir(),
        });
        // move the similarity matrix to the p-recs/sims location rather than leaving is in the tmp dir
        moveSimilarityMatrix();

        if(options.getDoXRecommender()){
            ToolRunner.run(getConf(), new XRecommenderJob(), new String[]{
                "--input", options.getAllActionsDir(),
                "--output", options.getSecondaryOutputDir(),
                "--similarityClassname", "SIMILARITY_LOGLIKELIHOOD",
                "--outputPathForSimilarityMatrix", options.getSecondarySimilarityMatrixPath(),
                "--tempDir", options.getSecondaryTempDir(),
                "--numUsers", Integer.toString(this.numberOfUsers),
                "--numItems", Integer.toString(this.numberOfItems),
                "--primaryPrefs", options.getPrimaryPrefsDir(),
                "--secondaryPrefs", options.getSecondaryPrefsDir(),
            });
        }

        return 0;
    }

    private static void cleanOutputDirs(Options options) throws IOException {
        FileSystem fs = FileSystem.get(new JobConf());
        //instead of deleting all, should delete only the ones we overwrite
        Path primaryOutputDir = new Path(options.getPrimaryOutputDir());
        try{
            fs.delete(primaryOutputDir, true);
        } catch (Exception e){
            LOGGER.info("No primary output dir to delete, skipping.");
        }
        Path secondaryOutputDir = new Path(options.getSecondaryOutputDir());
        try{
            fs.delete(secondaryOutputDir, true);
        } catch (Exception e){
            LOGGER.info("No secondary output dir to delete, skipping.");
        }
        //fs.mkdirs(new Path(options.getPrimaryRecsPath()));
        //fs.delete(new Path(options.getSimilarityMatrixPath()), true);
        try{
            fs.delete( new Path(options.getPrimaryRecsPath()), true);
        } catch (Exception e){
            LOGGER.info("No recs dir to delete, skipping.");
        }
        try{
            fs.delete(new Path(options.getTempDir()), true);
        } catch (Exception e){
            LOGGER.info("No temp dir to delete, skipping.");
        }
    }


    private static void moveSimilarityMatrix() throws IOException {
        FileSystem fs = FileSystem.get(new JobConf());
        Path from = new Path(options.getPrimarySimilarityMatrixPath());
        Path to = new Path(options.getPrimaryOutputDir(),XRecommenderJob.SIMS_MATRIX_PATH);//steal the path for Xrec though created by regular recommender
        fs.rename(from, to);
    }


    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new RecommenderDriverJob(), args);
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
        private static final String DEFAULT_PRIMARY_PREFS_PATH = "primary-prefs";
        private static final String DEFAULT_SECONDARY_PREFS_PATH = "secondary-prefs";
        private static final String DEFAULT_INDEXES_DIR = "id-indexes";
        private static final String DEFAULT_TEMP_DIR = "tmp";
        private static final String PRIMARY_TEMP_DIR = "tmp1";
        private static final String ROOT_RECS_DIR = "recs";
        private static final String SECONDARY_TEMP_DIR = "tmp2";
        private static final String PRIMARY_SIMILARITY_MATRIX = "similarityMatrix";//defined as a quoted String in mahout
        public static final String ROOT_SIMS_DIR = "sims";//defined as a quoted String in mahout
        private static final String DEFAULT_FILE_PATTERN = "part-";

        private int timestampColumn = DEFAULT_TIMESTAMP_COLUMN;
        private int userIDColumn = DEFAULT_UESERID_COLUMN;
        private int actionColumn = DEFAULT_ACTION_COLUMN;
        private int itemIDColumn = DEFAULT_ITEMID_COLUMN;
        private String action1 = DEFAULT_ACTION_1;
        private String action2 = DEFAULT_ACTION_2;
        private String action3 = DEFAULT_ACTION_3;
        private int numberOfRecsPerUser = 10;
        private String inputDir;//required
        private String outputDir = ROOT_RECS_DIR;
        private String tempDir = DEFAULT_TEMP_DIR;
        private String prefsDir = DEFAULT_PREFS_DIR;
        private Boolean doXRecommender = false;
        private String fileNamePatternString = DEFAULT_FILE_PATTERN;
        private String indexesDir = DEFAULT_INDEXES_DIR;
        private String primaryPrefsDir = DEFAULT_PRIMARY_PREFS_PATH;
        private String secondaryPrefsDir = DEFAULT_SECONDARY_PREFS_PATH;

        Options() {
        }

        public int getTimestampColumn() {
            return timestampColumn;
        }

        //todo: ignored so this is a stub. May wish to order perfs by timestamp before downsampling/truncating history
        public void setTimestampColumn(int timestampColumn) {
            this.timestampColumn = timestampColumn;
        }

        public int getUserIDColumn() {
            return userIDColumn;
        }

        @Option(name = "-uidc", aliases = { "--userIDColumn" }, usage = "Which column has the userID (optional). Default: 0", required = false)
        public void setUserIDColumn(int userIDColumn) {
            this.userIDColumn = userIDColumn;
        }

        public int getActionColumn() {
            return actionColumn;
        }

        @Option(name = "-ac", aliases = { "--actionColumn" }, usage = "Which column has the action (optional). Default: 1", required = false)
        public void setActionColumn(int actionColumn) {
            this.actionColumn = actionColumn;
        }

        public int getItemIDColumn() {
            return itemIDColumn;
        }

        @Option(name = "-iidc", aliases = { "--itemIDColumn" }, usage = "Which column has the itemID (optional). Default: 2", required = false)
        public void setItemIDColumn(int itemIDColumn) {
            this.itemIDColumn = itemIDColumn;
        }

        public String getAction1() {
            return action1;
        }

        @Option(name = "-a1", aliases = { "--action1" }, usage = "String respresenting action1, the primary preference action (optional). Default: 'purchase'", required = false)
        public void setAction1(String action1) {
            this.action1 = action1;
        }

        public String getAction2() {
            return action2;
        }

        @Option(name = "-a2", aliases = { "--action2" }, usage = "String respresenting action2, the secondary preference action (optional). Default: 'view'", required = false)
        public void setAction2(String action2) {
            this.action2 = action2;
        }

        public String getAction3() {
            return action3;
        }

        @Option(name = "-a3", aliases = { "--action3" }, usage = "String respresenting action3, used for splitting only (optional). Default: not used", required = false)
        public void setAction3(String action3) {
            this.action3 = action3;
        }

        public String getPrimaryPrefsDir() {
            return primaryPrefsDir;
        }

        public void setPrimaryPrefsDir(String primaryPrefsDir) {
            this.primaryPrefsDir = primaryPrefsDir;
        }

        public String getSecondaryPrefsDir() {
            return secondaryPrefsDir;
        }

        public void setSecondaryPrefsDir(String secondaryPrefsDir) {
            this.secondaryPrefsDir = secondaryPrefsDir;
        }

        public String getIndexesDir() {
            return indexesDir;
        }

        @Option(name = "-ix", aliases = { "--indexDir" }, usage = "Where to put user and item indexes (optional). Default: 'id-indexes'", required = false)
        public void setIndexesDir(String indexesDir) {
            this.indexesDir = indexesDir;
        }

        public String getFileNamePatternString() {
            return fileNamePatternString;
        }

        @Option(name = "-p", aliases = { "--inputFilePattern" }, usage = "Match this pattern when searching for action log files (optional). Default: '.tsv'", required = false)
        public void setFileNamePatternString(String fileNamePatternString) {
            this.fileNamePatternString = fileNamePatternString;
        }

        public Boolean getDoXRecommender() {
            return doXRecommender;
        }

        @Option(name = "-x", aliases = { "--xRecommend" }, usage = "Create cross-recommender for multiple actions (optional). Default: false.", required = false)
        public void setDoXRecommender(Boolean doXRecommender) {
            this.doXRecommender = doXRecommender;
        }

        private String getPrimaryRecsPath(){
            return new Path(getPrimaryOutputDir(),ROOT_RECS_DIR).toString();
        }

        private String getAllActionsDir(){
            return new Path(getInputDir(),getPrefsDir()).toString();
        }

        private String getPrimaryTempDir(){
            return new Path(getTempDir(),PRIMARY_TEMP_DIR).toString();
        }

        private String getSecondaryTempDir(){
            return new Path(getTempDir(),SECONDARY_TEMP_DIR).toString();
        }

        public String getPrefsDir() {
            return prefsDir;
        }

        public void setPrefsPath(String prefsDir) {
            this.prefsDir = prefsDir;
        }

        @Option(name = "-t", aliases = { "--tempDir" }, usage = "Place for intermediate data. Things left after the jobs but erased before starting new ones.", required = false)
        public void setTempPath(String tempDir) {
            this.tempDir = tempDir;
        }

        public String getTempDir() {
            return this.tempDir;
        }

       @Option(name = "-i", aliases = { "--inputDir" }, usage = "Input directory searched recursively for files in 'ExternalID' format where ID are unique strings and preference files contain combined actions with action IDs. Subdirs will be created by action type, so 'purchase', 'view', etc.", required = true)
        public void setInputDirPath(String primaryInputDir) {
            this.inputDir = primaryInputDir;
        }

        public String getInputDir() {
            return this.inputDir;
        }

        @Option(name = "-o", aliases = { "--outputDir" }, usage = "Output directory for recs. There will be two subdirs one for the primary recommender and one for the secondry/cross-recommender each of which will have item similarities and user history recs.", required = true)
        public void setOutputDir(String outputDir) {
            this.outputDir = outputDir;
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

        @Option(name = "-r", aliases = { "--recsPerUser" }, usage = "Number of recommendations to return for each request. Default = 10. Note: this option is ignored at present since a large number of recs are needed for some forms of blending. See the GetCassandraRecommendations job for limiting the number of recs.", required = false)
        public void setNumberOfRecsPerUser(int numberOfRecsPerUser) {
            this.numberOfRecsPerUser = numberOfRecsPerUser;
        }

        public int getNumberOfRecsPerUser() {
            return this.numberOfRecsPerUser;
        }

        public String getSimilairtyType() {
            return similairtyType;
        }

        @Option(name = "-s", aliases = { "--similarityType" }, usage = "Similarity measure to use. Default SIMILARITY_LOGLIKELIHOOD. Note: this only applies to the primary recs and secondary item similarities.", required = false)    public void setSimilairtyType(String similairtyType) {
            this.similairtyType = similairtyType;
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
