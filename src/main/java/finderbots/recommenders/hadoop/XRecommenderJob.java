package finderbots.recommenders.hadoop;

/**
 * User: pat
 * Date: 4/4/13
 * Time: 3:25 PM
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.math.hadoop.MatrixMultiplicationJob;
import org.apache.mahout.math.hadoop.TransposeJob;
import org.apache.mahout.math.hadoop.similarity.cooccurrence.RowSimilarityJob;
import org.apache.mahout.math.hadoop.similarity.cooccurrence.measures.VectorSimilarityMeasures;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * <p>Runs a completely distributed cross-recommender job as a series of mapreduces. The concept behind this is based on the fact that when implicit preferences are taken from user actions, rather than explicit preferences, it is useful to use one action for recommendation and the other will also work if the secondary action co-occurs with the first. For example views are predictive of purchases if the viewed item was indeed purchased.</p>
 * <p>A = matrix of views by user</p>
 * <p>B = matrix of purchases by user</p>
 * <p>[B'B]H_p = R_p, recommendations from purchase actions with strengths</p>
 * <p>[B'A]H_v = R_v, recommendations from view actions (where there was a purchase) with strengths</p>
 * <p>R_p + R_v = R, assuming a non-weighted linear combination</p>
 * <p>This job currently only calculates R_v since the usual RecommenderJob can be used to create R_p</p>
 * <p/>
 * <p>Further row similarities of [B'A]' will give item similarities for views with purchases and these are calculated by this job</p>
 * <p/>
 * <p>Preferences in the input file should look like {@code userID, itemID[, preferencevalue]}</p>
 * <p/>
 * <p>
 * Preference value is optional to accommodate applications that have no notion of a preference value (that is, the user
 * simply expresses a preference for an item, but no degree of preference). Boolean preferences have not been tested.
 * </p>
 * <p/>
 * <p>
 * The preference value is assumed to be parseable as a {@code double}. The user IDs and item IDs are
 * parsed as {@code long}s.
 * </p>
 * <p/>
 * <p>Command line arguments specific to this class are:</p>
 * <p/>
 * <ol>
 * <li>--input(path): Directory containing one or more text files with the preference data</li>
 * <li>--output(path): output path where recommender output should go</li>
 * <li>todo: --similarityClassname (classname): This is currently hardcoded to Log Likelihood</li>
 * <li>todo: does all recs currently --numRecommendations (integer): Number of recommendations to compute per user (10)</li>
 * <li>todo: --booleanData (boolean): Treat input data as having no pref values (false)</li>
 * <li>todo: does all similairties currently --maxSimilaritiesPerItem (integer): Maximum number of similarities considered per item (100)</li>
 * <li>todo: min = 1 --minPrefsPerUser (integer): ignore users with less preferences than this in the similarity computation (1)</li>
 * <li>todo: max = all --maxPrefsPerUserInItemSimilarity (integer): max number of preferences to consider per user in
 * the item similarity computation phase,
 * users with more preferences will be sampled down (1000)</li>
 * <li>todo: threshold not used, only number of recs applies --threshold (double): discard item pairs with a similarity value below this</li>
 * </ol>
 * <p/>
 * <p/>
 * <p>Note that because of how Hadoop parses arguments, all "-D" arguments must appear before all other
 * arguments.</p>
 */
public final class XRecommenderJob extends AbstractJob {

    public static final String BOOLEAN_DATA = "booleanData";

    private static final int DEFAULT_MAX_SIMILARITIES_PER_ITEM = 100;
    private static final int DEFAULT_MAX_PREFS_PER_USER = 1000;
    private static final int DEFAULT_NUM_RECOMMENDATIONS = 1000;
    private static final int DEFAULT_MAX_PREFS_PER_USER_CONSIDERED = 10;
    private static final int DEFAULT_MIN_PREFS_PER_USER = 1;

    private static final String MAX_PREFS_PER_USER_CONSIDERED = "maxPrefsPerUserConsidered";
    private static final String ITEMID_INDEX_PATH = "itemIDIndexPath";
    static final String NUM_RECOMMENDATIONS = "numRecommendations";

    private static final String CO_OCCURRENCE_MATRIX = "co-occurrence-matrix";
    private static final String RECS_MATRIX_PATH = "recs";
    public static final String SIMS_MATRIX_PATH = "sims";

    @Override
    public int run(String[] args) throws Exception {

        addInputOption();
        addOutputOption();
        addOption("numUsers", "nu", "Total number of user IDs seen for all actions.", true);
        addOption("numItems", "ni", "Total number of item IDs seen for all actions.", true);
        addOption("secondaryActionsDir", "sa", "Directory of preference files for secondary action",
            false);
        addOption("numRecommendations", "n", "Number of recommendations per user",
            String.valueOf(DEFAULT_NUM_RECOMMENDATIONS));
        addOption("maxPrefsPerUser", "mxp",
            "Maximum number of preferences considered per user in final recommendation phase",
            String.valueOf(DEFAULT_MAX_PREFS_PER_USER_CONSIDERED));
        addOption("minPrefsPerUser", "mp", "ignore users with less preferences than this in the similarity computation "
            + "(default: " + DEFAULT_MIN_PREFS_PER_USER + ')', String.valueOf(DEFAULT_MIN_PREFS_PER_USER));
        addOption("maxSimilaritiesPerItem", "m", "Maximum number of similarities considered per item ",
            String.valueOf(DEFAULT_MAX_SIMILARITIES_PER_ITEM));
        addOption("maxPrefsPerUserInItemSimilarity", "mppuiis", "max number of preferences to consider per user in the "
            + "item similarity computation phase, users with more preferences will be sampled down (default: "
            + DEFAULT_MAX_PREFS_PER_USER + ')', String.valueOf(DEFAULT_MAX_PREFS_PER_USER));
        addOption("similarityClassname", "s", "Name of distributed similarity measures class to instantiate, "
            + "alternatively use one of the predefined similarities (" + VectorSimilarityMeasures.list() + ')', true);
        addOption("threshold", "tr", "discard item pairs with a similarity value below this", false);
        addOption("outputPathForSimilarityMatrix", "opfsm", "write the item similarity matrix to this path (optional)",
            false);
        addOption("primaryPrefs", "pp", "Where to put the user prefs for Primary actions", true);
        addOption("secondaryPrefs", "sp", "Where to put the user prefs for Secondary actions", true);

        Map<String, List<String>> parsedArgs = parseArguments(args);
        if (parsedArgs == null) {
            return -1;
        }

        Path outputPath = getOutputPath();
        int numRecommendations = Integer.parseInt(getOption("numRecommendations"));
        boolean booleanData = false;
        int maxPrefsPerUser = Integer.parseInt(getOption("maxPrefsPerUser"));
        int minPrefsPerUser = Integer.parseInt(getOption("minPrefsPerUser"));
        int maxPrefsPerUserInItemSimilarity = Integer.parseInt(getOption("maxPrefsPerUserInItemSimilarity"));
        int maxSimilaritiesPerItem = Integer.parseInt(getOption("maxSimilaritiesPerItem"));
        String similarityClassname = getOption("similarityClassname");
        double threshold = hasOption("threshold")
            ? Double.parseDouble(getOption("threshold")) : RowSimilarityJob.NO_THRESHOLD;

        Path prepPath = getTempPath("prepareActionMatrixes");
        Path matrixATransposePath = new Path(prepPath, PrepareActionMatrixesJob.ACTION_A_TRANSPOSE_MATRIX_PATH);
        // matrix A is in
        Path matrixBTransposePath = new Path(prepPath, PrepareActionMatrixesJob.ACTION_B_TRANSPOSE_MATRIX_PATH);
        Path tempPath = getTempPath();
        JobConf conf = new JobConf();
        FileSystem fs = tempPath.getFileSystem(conf);

        int numberOfUsers = Integer.parseInt(getOption("numUsers"));
        int numberOfItems = Integer.parseInt(getOption("numItems"));

        // Ingest both actions into a DistributedRowMatrix(es) so create [A] for secondary actions and [B] for
        // primary actions
        ToolRunner.run(getConf(), new PrepareActionMatrixesJob(), new String[]{
            "--input", getInputPath().toString(),
            "--output", prepPath.toString(),
            "--maxPrefsPerUser", String.valueOf(maxPrefsPerUserInItemSimilarity),
            "--minPrefsPerUser", String.valueOf(minPrefsPerUser),
            "--booleanData", String.valueOf(booleanData),
            "--tempDir", tempPath.toString(),
            "--primaryPrefs", getOption("primaryPrefs"),
            "--secondaryPrefs", getOption("secondaryPrefs"),
        });

        // calculate the co-occurrence matrix [B'A]
        // matrix A is in

        // since the matrices were ingested and stored transposed we need to transpose again, just so the
        // multiply can transpose yet again - argh!
        ToolRunner.run(getConf(), new TransposeJob(), new String[]{
            "--input", matrixBTransposePath.toString(),
            "--numRows", Integer.toString(numberOfUsers),
            "--numCols", Integer.toString(numberOfItems),
            "--tempDir", tempPath.toString(),
        });
        Path matrixBPath = findMostRecentPath(prepPath, "transpose");

        // now get [A] from the ingested transposed version
        ToolRunner.run(getConf(), new TransposeJob(), new String[]{
            "--input", matrixATransposePath.toString(),
            "--numRows", Integer.toString(numberOfUsers),
            "--numCols", Integer.toString(numberOfItems),
            "--tempDir", tempPath.toString(),
        });
        Path matrixAPath = findMostRecentPath(prepPath, "transpose");


        // this actually does a matrixB.transpose.times(matrixA)

        ToolRunner.run(getConf(), new MatrixMultiplicationJob(), new String[]{
            "--numRowsA", Integer.toString(numberOfUsers),
            "--numColsA", Integer.toString(numberOfItems),
            "--numRowsB", Integer.toString(numberOfUsers),
            "--numColsB", Integer.toString(numberOfItems),
            "--inputPathA", matrixBPath.toString(),
            "--inputPathB", matrixAPath.toString(),
            // huh, no option of this name???? todo: do we have to find it after the mult?
            // "--outputPath", getTempPath(CO_OCCURRENCE_MATRIX).toString(),
            "--tempDir", tempPath.toString(),
        });
        //todo: output was put in tmp/productWith-168, really one directory up from the temp passed in?
        Path beforeCooccurrenceMatrixPath = findMostRecentPath(tempPath.getParent(), "product");
        Path cooccurrenceMatrixPath = new Path(tempPath, beforeCooccurrenceMatrixPath.getName());
        fs.rename(beforeCooccurrenceMatrixPath, cooccurrenceMatrixPath);

        // now [B'A] will be transposed before the multiply so we need to transpose twice?
        // calculating [B'A]H_v by first transposing the [B'A] then creating the multiply job but
        // H_v is A' (view history vectors are column vectors) so we have to transpose both [B'A] and A
        // since the multiply with transpose the first matrix (not sure why but it does).

        ToolRunner.run(getConf(), new TransposeJob(), new String[]{
            "--input", cooccurrenceMatrixPath.toString(),
            "--numRows", Integer.toString(numberOfUsers),
            "--numCols", Integer.toString(numberOfItems),
            "--tempDir", tempPath.toString(),
        });
        //"--input", getTempPath(CO_OCCURRENCE_MATRIX).toString(),

        Path transposedBTransposeAMatrixPath = findMostRecentPath(tempPath, "transpose");


        ToolRunner.run(getConf(), new MatrixMultiplicationJob(), new String[]{
            "--inputPathA", transposedBTransposeAMatrixPath.toString(),//[B'A]'
            "--numRowsA", Integer.toString(numberOfItems),
            "--numColsA", Integer.toString(numberOfItems),
            "--inputPathB", matrixATransposePath.toString(),
            "--numRowsB", Integer.toString(numberOfItems),
            "--numColsB", Integer.toString(numberOfUsers),
            "--tempDir", tempPath.toString(),
        });
        //"--outputPath", getOutputPath(RECS_MATRIX_PATH).toString(),

        Path recsMatrixPath = findMostRecentPath(tempPath, "product");

        // co-occurrence matrix already transposed into rows = v-items for item similairty
        // in transposedBTransposeAMatrixPath so calc similar items from it
        Path similarItemsPath = new Path(outputPath, XRecommenderJob.SIMS_MATRIX_PATH);
        ToolRunner.run(getConf(), new RowSimilarityJob(), new String[]{
            "--input", transposedBTransposeAMatrixPath.toString(),
            "--output", similarItemsPath.toString(),
            "--numberOfColumns", String.valueOf(numberOfUsers),
            "--similarityClassname", similarityClassname,
            "--maxSimilaritiesPerRow", String.valueOf(maxSimilaritiesPerItem),
            "--excludeSelfSimilarity", String.valueOf(Boolean.TRUE),
            "--threshold", String.valueOf(threshold),
            "--tempDir", tempPath.toString(),
        });

        // todo: now move the cooccurrence and recommendations matrixes to the output path

        return 0;
    }

    private Path findMostRecentPath(Path where, String what) throws IOException {
        JobConf conf = new JobConf();
        FileSystem fs = where.getFileSystem(conf);

        FileStatus[] files = fs.listStatus(where);// sorted by creations timestamp?
        Long time = Long.MIN_VALUE;
        FileStatus newest = null;
        for (FileStatus fstat : files) {
            if (fstat.getPath().toString().contains(what)) {
                time = fstat.getModificationTime();
                if ((newest != null && newest.getModificationTime() < time) || (newest == null)) {
                    newest = fstat;
                }
            }
        }
        return (newest != null) ? newest.getPath() : null;
    }

    private static void setIOSort(JobContext job) {
        Configuration conf = job.getConfiguration();
        conf.setInt("io.sort.factor", 100);
        String javaOpts = conf.get("mapred.map.child.java.opts"); // new arg name
        if (javaOpts == null) {
            javaOpts = conf.get("mapred.child.java.opts"); // old arg name
        }
        int assumedHeapSize = 512;
        if (javaOpts != null) {
            Matcher m = Pattern.compile("-Xmx([0-9]+)([mMgG])").matcher(javaOpts);
            if (m.find()) {
                assumedHeapSize = Integer.parseInt(m.group(1));
                String megabyteOrGigabyte = m.group(2);
                if ("g".equalsIgnoreCase(megabyteOrGigabyte)) {
                    assumedHeapSize *= 1024;
                }
            }
        }
        // Cap this at 1024MB now; see https://issues.apache.org/jira/browse/MAPREDUCE-2308
        conf.setInt("io.sort.mb", Math.min(assumedHeapSize / 2, 1024));
        // For some reason the Merger doesn't report status for a long time; increase
        // timeout when running these jobs
        conf.setInt("mapred.task.timeout", 60 * 60 * 1000);
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new XRecommenderJob(), args);
    }
}
