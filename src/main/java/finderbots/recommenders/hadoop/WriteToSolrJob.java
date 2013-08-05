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
 * <p>Writes the DRMs passed in to Solr as csv files to a location in HDFS or the local file system. The Primary DRM is expected to be a item-item similarity matrix with Mahout internal ID. The Secondary DRM is from cross-action-similarities. It also needs a file containing a map of internal mahout IDs to external IDs--one for userIDs and one for itemIDs. It needs the location to put the similarity matrices, each will be put into a Solr fields of type 'string' for indexing.</p>
 * <p>The Solr csv files will be of the form:</p>
 * <p>item_id,similar_items,cross_action_similar_items</p>
 * <p> ipad,iphone,iphone nexus</p>
 * <p> iphone,ipad,ipad galaxy</p>
 * <p>todo: This is in-memory and single threaded. It's easy enough to mapreduce it but there would still have to be a shared in-memory BiMap per node. To remove the in-memory map a more complex data flow with joins needs to be implemented.</p>
 * <p>todo: Solr and LucidWorks Search support many stores for indexing. It might be nice to have a pluggable writer for different stores.</p>
 */

import com.google.common.collect.BiMap;
import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public final class WriteToSolrJob extends Configured implements Tool {
    private static Logger LOGGER = Logger.getRootLogger();

    private static Options options;
    private BiMap<String, String> userIndex;
    private BiMap<String, String> itemIndex;
    FileSystem fs;

    @Override
    public int run(String[] args) throws Exception {
        options = new Options();
        CmdLineParser parser = new CmdLineParser(options);
        String s = options.toString();// for debuging ease

        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
            return -1;
        }
        fs = FileSystem.get(getConf());

        cleanOutputDirs();

        Path itemIndexPath = new Path(options.getItemIndexFilePath());
        Path userIndexPath = new Path(options.getUserIndexFilePath());
        Path itemSimilarityMatrixPath = new Path(options.getItemSimilarityMatrixDir());
        Path crossActionSimilarityMatrixPath = new Path(options.getCrossSimilarityMatrixDir());
        Path solrSimilaritiesDocsFilePath = new Path(options.getSolrItemsSimilaritiesDocFilePath());
        FSDataOutputStream solrSimilaritiesDocsFile = fs.create(solrSimilaritiesDocsFilePath);

        itemIndex = Utils.readIndex(itemIndexPath );
        //writing the similarities does not require the user index
        writeDRMasCSV(itemIndex, userIndex, itemSimilarityMatrixPath, solrSimilaritiesDocsFile);
        return 0;
    }

    private void writeDRMasCSV( BiMap<String, String> itemIndex, BiMap<String, String> userIndex, Path mahoutDRMPath, FSDataOutputStream outFile){


    }

    private void cleanOutputDirs() throws IOException {
        //todo: instead of deleting all, delete only the ones we overwrite?
        Path outputDir = new Path(options.getOutputDir());
        Path solrItemsSimilaritiesDocsDir = new Path(options.getSolrItemSimilaritiesDocsDir());
        Path solrItemsSimilaritiesDocsFilePath = new Path(options.getSolrItemsSimilaritiesDocFilePath());
        if(!fs.exists(outputDir) || !fs.exists(solrItemsSimilaritiesDocsDir)){
            fs.mkdirs(solrItemsSimilaritiesDocsDir);//will create outputDir too if needed
        } else {//clean existing ones
            try {
                fs.delete(solrItemsSimilaritiesDocsFilePath, true);
            } catch (Exception e) {
                LOGGER.info("No docs file yet.");
            }
        }
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new WriteToSolrJob(), args);
    }

    // Command line options for this job. Execute the main method above with no parameters
    // to get a help listing.
    //

    public class Options {

        //used by Solr
        private static final String DEFAULT_ITEM_ID_FIELD_NAME = "item_id";
        private static final String DEFAULT_USER_ID_FIELD_NAME = "user_id";
        private static final String DEFAULT_ITEM_SIMILARITY_FIELD_NAME = "similar_items";
        private static final String DEFAULT_CROSS_ITEM_SIMILARITY_FIELD_NAME = "cross_action_similar_items";
        private static final String DEFAULT_ITEM_INDEX_FILENAME = ActionSplitterJob.Options.DEFAULT_ITEM_INDEX_FILENAME;
        private static final String DEFAULT_USER_INDEX_FILENAME = ActionSplitterJob.Options.DEFAULT_USER_INDEX_FILENAME;
        private static final String DEFAULT_SOLR_ITEM_SIMILARITIES_DOCS_DIR = "item-similarities-docs";
        private static final String DEFAULT_SOLR_ITEM_SIMILARITIES_DOCS_FILENAME = "item-similarities.csv";
        private static final String DEFAULT_SOLR_USER_ACTIONS_DOCS_DIR = "user-history-docs";
        private String itemSimilarityMatrixDir;//required
        private String crossSimilarityMatrixDir = "";//optional
        private String userAction1HistoryMatrixDir;//required
        private String userAction2HistoryMatrixDir;//required
        private String indexesDir;//required
        private String userIndexFilePath;
        private String itemIndexFilePath;
        private String outputDir;//required
        private String itemIdFieldName = DEFAULT_ITEM_ID_FIELD_NAME;
        private String userIdFieldName = DEFAULT_USER_ID_FIELD_NAME;
        private String itemSimilarityFieldName = DEFAULT_ITEM_SIMILARITY_FIELD_NAME;
        private String crossActionSimilarityFieldName = DEFAULT_CROSS_ITEM_SIMILARITY_FIELD_NAME;
        private String solrItemSimilaritiesDocsDir;//derived from requied output dir
        private String solrItemsSimilaritiesDocFilePath;//derived from required stuff


        Options() {
        }

        public String getUserAction1HistoryMatrixDir() {
            return userAction1HistoryMatrixDir;
        }

        @Option(name = "-ua1", aliases = {"--userAction1MatrixDir"}, usage = "Input directory containing the Mahout DistributedRowMatrix with users' action1 history (optional).", required = false)
        public void setUserAction1HistoryMatrixDir(String userAction1HistoryMatrixDir) {
            this.userAction1HistoryMatrixDir = userAction1HistoryMatrixDir;
        }

        public String getUserAction2HistoryMatrixDir() {
            return userAction2HistoryMatrixDir;
        }

        public void setUserAction2HistoryMatrixDir(String userAction2HistoryMatrixDir) {
            this.userAction2HistoryMatrixDir = userAction2HistoryMatrixDir;
        }

        public String getItemIdFieldName() {
            return itemIdFieldName;
        }

        public String getUserIdFieldName() {
            return userIdFieldName;
        }

        public String getItemSimilarityFieldName() {
            return itemSimilarityFieldName;
        }

        public String getCrossActionSimilarityFieldName() {
            return crossActionSimilarityFieldName;
        }

        public String getItemSimilarityMatrixDir() {
            return itemSimilarityMatrixDir;
        }

        @Option(name = "-ism", aliases = {"--itemSimilarityMatrixDir"}, usage = "Input directory containing the Mahout DistributedRowMatrix with Item-Item similarities.", required = true)
        public void setItemSimilarityMatrixDir(String itemSimilarityMatrixDir) {
            this.itemSimilarityMatrixDir = itemSimilarityMatrixDir;
        }

        public String getIndexesDir() {
            return indexesDir;
        }

        @Option(name = "-ix", aliases = {"--indexDir"}, usage = "Directory containing user and item indexes.", required = true)
        public void setIndexesDir(String indexesDir) {
            this.indexesDir = indexesDir;
            if(this.userIndexFilePath == null)
                userIndexFilePath = new Path(indexesDir, DEFAULT_USER_INDEX_FILENAME).toString();
            if(this.itemIndexFilePath == null)
                itemIndexFilePath = new Path(indexesDir, DEFAULT_ITEM_INDEX_FILENAME).toString();
        }

        public String getCrossSimilarityMatrixDir() {
            return crossSimilarityMatrixDir;
        }

        @Option(name = "-csm", aliases = {"--crossSimilarityMatrixDir"}, usage = "Input directory containing the Mahout DistributedRowMatrix with Item-Item cross-action similarities.", required = true)
        public void setCrossSimilarityMatrixDir(String crossSimilarityMatrixDir) {
            this.crossSimilarityMatrixDir = crossSimilarityMatrixDir;
        }

        public String getUserIndexFilePath() {
            return userIndexFilePath;
        }

        @Option(name = "-uix", aliases = {"--userIndex"}, usage = "Input directory containing the serialized BiMap of Mahout ID <-> external ID (optional, overrides --indexDir). Default: indexDir/user-index.", required = false)
        public void setUserIndexFilePath(String userIndexFilePath) {
            this.userIndexFilePath = userIndexFilePath;
        }

        public String getItemIndexFilePath() {
            return itemIndexFilePath;
        }

        @Option(name = "-iix", aliases = {"--itemIndex"}, usage = "Input directory containing the serialized BiMap of Mahout ID <-> external ID (optional, overrides --indexDir). Default: indexDir/item-index", required = false)
        public void setItemIndexFilePath(String itemIndexFilePath) {
            this.itemIndexFilePath = itemIndexFilePath;
        }

        public String getOutputDir() {
            return outputDir;
        }

        @Option(name = "-o", aliases = {"--output"}, usage = "Where to write docs of ids for indexing. Danger: will be cleaned before writing!", required = true)
        public void setOutputDir(String outputDir) {
            this.outputDir = outputDir;
            this.solrItemSimilaritiesDocsDir = new Path(new Path(this.outputDir), DEFAULT_SOLR_ITEM_SIMILARITIES_DOCS_DIR).toString();
            this.solrItemsSimilaritiesDocFilePath = new Path(this.solrItemSimilaritiesDocsDir, DEFAULT_SOLR_ITEM_SIMILARITIES_DOCS_FILENAME).toString();

        }

        public String getSolrItemSimilaritiesDocsDir() {
            return solrItemSimilaritiesDocsDir;
        }

        public String getSolrItemsSimilaritiesDocFilePath() {
            return solrItemsSimilaritiesDocFilePath;
        }
/* not needed?
        @Option(name = "-t", aliases = {"--tempDir"}, usage = "Place for intermediate data. Things left after the jobs but erased before starting new ones.", required = false)
        public void setTempDir(String tempDir) {
            this.tempDir = tempDir;
        }

        public String getTempDir() {
            return this.tempDir;
        }

        */

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
