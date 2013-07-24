package finderbots.recommenders.hadoop;

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

public final class PinterestRecommenderDriverJob extends AbstractJob {
    private static Logger LOGGER = Logger.getRootLogger();

    public static final String PREFS_ROOT_DIR = "prefs";
    public static final String ID_INDEXES_PATH = "id-indexes";
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
        if (options.getUseExternalIDFiles()) {//todo: using raw mahout ided preference files is not tested
            Path prefFilesRootDir = new Path(options.getOutputDirPath());
            FileSystem fs = prefFilesRootDir.getFileSystem(getConf());
            Path indexesPath = new Path(prefFilesRootDir, ID_INDEXES_PATH);
            Path prefsPath = new Path(prefFilesRootDir, PREFS_ROOT_DIR);
            options.setPrefsPath(prefsPath.toString());

            if(!options.getDoNotSplit()){//if this is false we are splitting
                //todo: this can be made generic if the following values are passed in
                final String ACTION_1 = "FOLLOWS";
                final String ACTION_2 = "FOLLOWED_BY";
                final String ACTION_3 = "REPINNED_USER";
                final String ACTION_COLUMN = "2";
                final String TIMESTAMP_COLUMN = "0";//todo: -1 = not used
                final String ITEM_ID_COLUMN = "3";
                final String USER_ID_COLUMN = "1";
                final String INPUT_DELIMETER = "\t";
                final String OUTPUT_DELIMETER = ",";

                // split into actions and store in subdirs
                // create and index for users and another for items
                // this job cleans out the output dir first
                if(options.getMergePrimaryAndSecondaryActions()){
                    ToolRunner.run(getConf(), new ActionSplitterJob(), new String[]{
                        "--inputDir", options.getInputDirPath(),
                        "--outputDir", prefsPath.toString(),
                        "--indexesDir", indexesPath.toString(),
                        "--mergeAction1AndAction2",
                        "--action1", ACTION_1,
                        "--action2", ACTION_2,
                        "--action3", ACTION_3,
                        "--inputDelim", INPUT_DELIMETER,
                        "--outputDelim", OUTPUT_DELIMETER,
                        "--actionCol", ACTION_COLUMN,
                        "--itemCol", ITEM_ID_COLUMN,
                        "--userCol", USER_ID_COLUMN,
                        "--timestampCol", TIMESTAMP_COLUMN,
                    });
                } else { // keep primary and secondary actions separate
                    ToolRunner.run(getConf(), new ActionSplitterJob(), new String[]{
                        "--inputDir", options.getInputDirPath(),
                        "--outputDir", prefsPath.toString(),
                        "--indexesDir", indexesPath.toString(),
                        "--action1", ACTION_1,
                        "--action2", ACTION_2,
                        "--action3", ACTION_3,
                        "--inputDelim", INPUT_DELIMETER,
                        "--outputDelim", OUTPUT_DELIMETER,
                        "--actionCol", ACTION_COLUMN,
                        "--itemCol", ITEM_ID_COLUMN,
                        "--userCol", USER_ID_COLUMN,
                        "--timestampCol", TIMESTAMP_COLUMN,
                    });
                }
            }// otherwise assume we have already split

            // need to get the number of users and items from the splitter, which also creates indexes
            this.numberOfUsers = HadoopUtil.readInt(new Path(indexesPath, ActionSplitterJob.getNumberOfUsersFile()), getConf());
            this.numberOfItems = HadoopUtil.readInt(new Path(indexesPath, ActionSplitterJob.getNumberOfItemsFile()), getConf());
            // these are single value binary files written with
            // HadoopUtil.writeInt(this.numberOfUsers, getOutputPath(NUM_USERS), getConf());

            options.setInputDirPath(prefFilesRootDir.toString());
            options.setPrimaryPrefsPath(ActionSplitterJob.getAction1Dir());//primary actions
            options.setSecondaryPrefsPath(ActionSplitterJob.getAction2Dir());// secondary actions
        }



        ToolRunner.run(getConf(), new RecommenderJob(), new String[]{
            "--input", options.getPrimaryActionsPath(),
            "--output", options.getPrimaryRecsPath(),
            "--similarityClassname", options.getSimilairtyType(),
            //this options creates a text file from the similarity matrix--not needed if you want to use the sequencefile
            //which is already in the temp dir.
//            "--outputPathForSimilarityMatrix", options.getPrimarySimilarityMatrixPath(),
            "--tempDir", options.getPrimaryTempPath(),
        });
        // move the similarity matrix to the p-recs/sims location rather than leaving is in the tmp dir
        moveSimilarityMatrix();

        if(options.getDoXRecommender()){
            ToolRunner.run(getConf(), new XRecommenderJob(), new String[]{
                "--input", options.getAllActionsPath(),
                "--output", options.getSecondaryOutputDirPath(),
                "--similarityClassname", "SIMILARITY_LOGLIKELIHOOD",
                "--outputPathForSimilarityMatrix", options.getSecondarySimilarityMatrixPath(),
                "--tempDir", options.getSecondaryTempPath(),
                "--numUsers", Integer.toString(this.numberOfUsers),
                "--numItems", Integer.toString(this.numberOfItems),
                "--primaryPrefs", options.getPrimaryPrefsPath(),
                "--secondaryPrefs", options.getSecondaryPrefsPath(),

            });
        }

        return 0;
    }

    private static void cleanOutputDirs(Options options) throws IOException {
        FileSystem fs = FileSystem.get(new JobConf());
        //instead of deleting all, should delete only the ones we overwrite
        Path primaryOutputDir = new Path(options.getPrimaryOutputDirPath());
        try{
            fs.delete(primaryOutputDir, true);
        } catch (Exception e){
            LOGGER.info("No primary output dir to delete, skipping.");
        }
        Path secondaryOutputDir = new Path(options.getSecondaryOutputDirPath());
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
            fs.delete(new Path(options.getTempPath()), true);
        } catch (Exception e){
            LOGGER.info("No temp dir to delete, skipping.");
        }
    }


    private static void moveSimilarityMatrix() throws IOException {
        FileSystem fs = FileSystem.get(new JobConf());
        Path from = new Path(options.getPrimarySimilarityMatrixPath());
        Path to = new Path(options.getPrimaryOutputDirPath(),XRecommenderJob.SIMS_MATRIX_PATH);//steal the path for Xrec though created by regular recommender
        fs.rename(from, to);
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new PinterestRecommenderDriverJob(), args);
    }

    // Command line options for this job. Execute the main method above with no parameters
    // to get a help listing.
    //

    public class Options {

        public static final String USER_ONLY = "USER-ONLY";
        public static final String ITEM_ONLY = "ITEM-ONLY";
        public static final String USER_ITEM = "USER-ITEM";
        public static final String ITEM_USER = "ITEM-USER";

        private String similairtyType = LOG_LIKELIHOOD;//hard coded to log-likelihood
        public static final String LOG_LIKELIHOOD = "SIMILARITY_LOGLIKELIHOOD";
        public static final String EUCLIDEAN = "SIMILARITY_EUCLIDEAN";
        public static final String COSINE = "SIMILARITY_COSINE";
        public static final String TANIMOTO = "SIMILARITY_TANIMOTO_COEFFICIENT";
        public static final String CITY_BLOCK = "SIMILARITY_CITY_BLOCK";


        private static final String PRIMARY_OUTPUT_PATH = "primary-recs";
        private static final String SECONDARY_OUTPUT_PATH = "secondary-recs";
        private static final String PRIMARY_PREFS_PATH = "primary-prefs";
        private static final String SECONDARY_PREFS_PATH = "secondary-prefs";
        private static final String DEFAULT_PREFS_PATH = "prefs";
        private static final String DEFAULT_TEMP_PATH = "tmp";
        private static final String PRIMARY_TEMP_DIR = "tmp1";
        private static final String ROOT_RECS_DIR = "recs";
        private static final String SECONDARY_TEMP_DIR = "tmp2";
        private static final String PRIMARY_SIMILARITY_MATRIX = "similarityMatrix";//defined as a quoted String in mahout
        public static final String ROOT_SIMS_DIR = "sims";//defined as a quoted String in mahout
        private int numberOfRecsPerUser = 10;
        private String inputDirPath = "";
        private String outputDirPath = "";
        private String tempPath = DEFAULT_TEMP_PATH;
        private String prefsPath = DEFAULT_TEMP_PATH;
        private String primaryPrefsPath;
        private String secondaryPrefsPath;
        private Boolean useExternalIDFiles;
        private Boolean doXRecommender;
        private Boolean doNotSplit;
        private Boolean mergePrimaryAndSecondaryActions;

        Options() {
            this.similairtyType = LOG_LIKELIHOOD;
            this.outputDirPath = ROOT_RECS_DIR;
            this.tempPath = DEFAULT_TEMP_PATH;
            this.prefsPath = DEFAULT_TEMP_PATH;
            //use path relative to prefsPath
            this.primaryPrefsPath =  PRIMARY_PREFS_PATH;
            this.secondaryPrefsPath = SECONDARY_PREFS_PATH;
            this.useExternalIDFiles = true;
            this.doXRecommender = false;
            this.doNotSplit = false;//default to do the splitting
            this.mergePrimaryAndSecondaryActions = false;//split these, merging them will produce higher map scores usually
        }


        public String getPrimaryPrefsPath() {
            return primaryPrefsPath;
        }

        public void setPrimaryPrefsPath(String primaryPrefsPath) {
            this.primaryPrefsPath = primaryPrefsPath;
        }

        public String getSecondaryPrefsPath() {
            return secondaryPrefsPath;
        }

        public void setSecondaryPrefsPath(String secondaryPrefsPath) {
            this.secondaryPrefsPath = secondaryPrefsPath;
        }

        public Boolean getMergePrimaryAndSecondaryActions() {
            return mergePrimaryAndSecondaryActions;
        }

        @Option(name = "-mergePrimaryAndSecondaryActions", usage = "Treat secondary action exactly as the primary action for training (optional). Default: false", required = false)
        public void setMergePrimaryAndSecondaryActions(Boolean mergePrimaryAndSecondaryActions) {
            this.mergePrimaryAndSecondaryActions = mergePrimaryAndSecondaryActions;
        }

        public Boolean getDoNotSplit() {
            return doNotSplit;
        }

        @Option(name = "-doNotSplit", usage = "Do not read in externalid files and split them by action type (optional). Default: false so do the splitting.", required = false)
        public void setDoNotSplit(Boolean doNotSplit) {
            this.doNotSplit = doNotSplit;
        }

        public Boolean getDoXRecommender() {
            return doXRecommender;
        }

        @Option(name = "-xRecommend", usage = "Create cross-recommender for multiple actions (optional). Default: false.", required = false)
        public void setDoXRecommender(Boolean doXRecommender) {
            this.doXRecommender = doXRecommender;
        }

        private String getPrimaryRecsPath(){
            return new Path(getPrimaryOutputDirPath(),ROOT_RECS_DIR).toString();
        }

        private String getPrimaryActionsPath(){
            return new Path(new Path(getInputDirPath(),PREFS_ROOT_DIR), ActionSplitterJob.getAction1Dir()).toString();
        }

        private String getAllActionsPath(){
            return new Path(getInputDirPath(),PREFS_ROOT_DIR).toString();
        }

        private String getPrimaryTempPath(){
            return new Path(getTempPath(),PRIMARY_TEMP_DIR).toString();
        }

        private String getSecondaryTempPath(){
            return new Path(getTempPath(),SECONDARY_TEMP_DIR).toString();
        }

        public String getPrefsPath() {
            return prefsPath;
        }

        public void setPrefsPath(String prefsPath) {
            this.prefsPath = prefsPath;
        }

        @Option(name = "-tempDir", usage = "Place for intermediate data. Things left after the jobs but erased before starting new ones.", required = false)
        public void setTempPath(String tempPath) {
            this.tempPath = tempPath;
        }

        public String getTempPath() {
            return this.tempPath;
        }

        @Option(name = "-useExternalIDFiles", usage = "Use file that has all external string user and item ids and are called 'part-xxxx'. Treat the input dir as a place to recursively look for these files (optional). Default: true and false probably will not work. The idea is to allow raw Mahout csv files as input.", required = false)
        public void setUseExternalIDFiles(Boolean useExternalIDFiles) {
            this.useExternalIDFiles = useExternalIDFiles;
        }

        public Boolean getUseExternalIDFiles() {
            return useExternalIDFiles;
        }

       @Option(name = "-inputDir", usage = "Input directory searched recursively for files in 'ExternalID' format where IDs are unique strings and preference files contain combined actions with action IDs. Subdirs will be created and split by action type", required = true)
        public void setInputDirPath(String primaryInputDirPath) {
            this.inputDirPath = primaryInputDirPath;
        }

        public String getInputDirPath() {
            return this.inputDirPath;
        }

        @Option(name = "-outputDir", usage = "Output directory for recs. There will be two subdirs one for the primary recommender and one for the secondry/cross-recommender each of which will have item similarities and user history recs.", required = true)
        public void setOutputDirPath(String outputDirPath) {
            this.outputDirPath = outputDirPath;
        }

        public String getOutputDirPath() {
            return this.outputDirPath;
        }

        public String getPrimaryOutputDirPath() {
            return new Path(this.outputDirPath, PRIMARY_OUTPUT_PATH).toString();
        }

        public String getSecondaryOutputDirPath() {
            return new Path(this.outputDirPath, SECONDARY_OUTPUT_PATH).toString();
        }

        @Option(name = "-recsPerUser", usage = "Number of recommendations to return for each request. Default = 10. Note: this option is ignored at present since a large number of recs are needed for some forms of blending. See the GetCassandraRecommendations job for limiting the number of recs.", required = false)
        public void setNumberOfRecsPerUser(int numberOfRecsPerUser) {
            this.numberOfRecsPerUser = numberOfRecsPerUser;
        }

        public int getNumberOfRecsPerUser() {
            return this.numberOfRecsPerUser;
        }

        public String getSimilairtyType() {
            return similairtyType;
        }

        @Option(name = "-similarityType", usage = "Similarity measure to use. Default SIMILARITY_LOGLIKELIHOOD. Note: this only applies to the primary recs and secondary item similarities.", required = false)    public void setSimilairtyType(String similairtyType) {
            this.similairtyType = similairtyType;
        }

        public String getPrimarySimilarityMatrixPath() {
            return new Path(getPrimaryTempPath(), PRIMARY_SIMILARITY_MATRIX).toString();
        }

        public String getSecondarySimilarityMatrixPath() {
            return new Path(getSecondaryOutputDirPath(), ROOT_SIMS_DIR).toString();
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
