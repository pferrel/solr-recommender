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
 * <p>todo: There are two shared in-memory BiHashMaps per node. To remove the in-memory maps a more complex data flow needs to be implemented.</p>
 * <p>todo: Solr and LucidWorks Search support many stores for indexing. It might be nice to have a pluggable writer for different stores.</p>
 */

import com.google.common.collect.BiMap;
import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.mahout.math.Vector;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

public class WriteToSolrJob extends Configured implements Tool {
    private static Logger LOGGER = Logger.getRootLogger();

    private static Options options;
    FileSystem fs;

    /* This class joins A and B by user ID and writes the data to a set of CSV files with the following headers:
    * id,b_history,a_history
    *
    * It joins [B'B] and [B'A] by item ID and writes the data as a set of csv files with the following headers:
    * id,b_b_links,b_a_links
    */
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
        Map fields = new HashMap<String, String>();

        //inputs
        Path bTransposeByMatrixPath = new Path(options.getBTransposeBMatrixDir());
        Path bUserHistoryMatrixPath = new Path(options.getBUserHistoryMatrixDir());

        //outputs
        Path solrItemsLinksDocsFilesPath = new Path(options.getSolrItemLinksDocsDir());
        Path solrUserHistoryDocsFilesPath = new Path(options.getSolrUserHistoryDir());

        if(options.getBTransposeAMatrixDir() != null && options.getAUserHistoryMatrixDir() != null){
            //optional inputs
            Path bTransposeAMatrixPath = new Path(options.getBTransposeAMatrixDir());
            Path aUserHistoryMatrixPath = new Path(options.getAUserHistoryMatrixDir());



            fields.put("iD1", options.getItemIdFieldName());
            fields.put("dRM1FieldName", options.getBTranposeBFieldName());
            fields.put("dRM2FieldName", options.getBTransposeAFieldName());
            WriteDRMsToSolr join = new WriteDRMsToSolr(fields);
            join.joinDRMsWriteToSolr(itemIndexPath, itemIndexPath, bTransposeByMatrixPath, bTransposeAMatrixPath, solrItemsLinksDocsFilesPath);

            fields.clear();
            fields.put("iD1", options.getUserIdFieldName());
            fields.put("dRM1FieldName", options.getBUserHistoryFieldName());
            fields.put("dRM2FieldName", options.getAUserHistoryFieldName());
            join = new WriteDRMsToSolr(fields);
            join.joinDRMsWriteToSolr(userIndexPath, itemIndexPath, bUserHistoryMatrixPath, aUserHistoryMatrixPath, solrUserHistoryDocsFilesPath);
        } else { //only using B actions so no CoGroup join required
            fields.put("iD1", options.getItemIdFieldName());
            fields.put("dRM1FieldName", options.getBTranposeBFieldName());
            WriteDRMsToSolr join = new WriteDRMsToSolr(fields);
            join.writeDRMToSolr(itemIndexPath, itemIndexPath, bTransposeByMatrixPath, solrItemsLinksDocsFilesPath);

            fields.clear();
            fields.put("iD1", options.getUserIdFieldName());
            fields.put("dRM1FieldName", options.getBUserHistoryFieldName());
            join = new WriteDRMsToSolr(fields);
            join.writeDRMToSolr(userIndexPath, itemIndexPath, bUserHistoryMatrixPath, solrUserHistoryDocsFilesPath);
        }
        return 0;
    }

    private String getOrderedItems( Vector v, BiMap<String, String> elementIndex){
        String doc = new String("");
        //sort the vector by element weight
        class VectorElementComparator implements Comparator<Vector.Element> {

            @Override
            public int compare(Vector.Element o1, Vector.Element o2) {
                return (o1.get() > o2.get() ? -1 : (o1.equals(o2) ? 0 : 1));
            }
        }

        ArrayList<Vector.Element> vel = new ArrayList<Vector.Element>();
        for(Vector.Element ve : v.nonZeroes()) vel.add(ve);
        Collections.sort(vel, new VectorElementComparator());
        for(Vector.Element ve : vel){
            int i = ve.index();
            String s = String.valueOf(i);
            String exID = elementIndex.inverse().get(s);
            String intID = elementIndex.get(s);
            doc += exID+" ";
        }
        return doc;
    }

    private void cleanOutputDirs() throws IOException {
        //delete only the ones we want to overwrite
        Path solrItemsSimilaritiesDocsDir = new Path(options.getSolrItemLinksDocsDir());
        Path solrUserHistoryDocsDir = new Path(options.getSolrUserHistoryDir());
        if(fs.exists(solrItemsSimilaritiesDocsDir))
            fs.delete(solrItemsSimilaritiesDocsDir, true);
        if(fs.exists(solrUserHistoryDocsDir))
            fs.delete(solrUserHistoryDocsDir, true);
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new WriteToSolrJob(), args);
    }

    // Command line options for this job. Execute the main method above with no parameters
    // to get a help listing.
    //

    public class Options {

        //used by Solr
        private static final String DEFAULT_ITEM_ID_FIELD_NAME = "id";
        private static final String DEFAULT_USER_ID_FIELD_NAME = "id";//id is a Solr required field unique per index
        private static final String DEFAULT_B_TRANSPOSE_B_FIELD_NAME = "b_b_links";
        private static final String DEFAULT_B_TRANSPOSE_A_FIELD_NAME = "b_a_links";
        private static final String DEFAULT_ITEM_INDEX_FILENAME = ActionSplitterJob.Options.DEFAULT_ITEM_INDEX_FILENAME;
        private static final String DEFAULT_USER_INDEX_FILENAME = ActionSplitterJob.Options.DEFAULT_USER_INDEX_FILENAME;
        private static final String DEFAULT_JOINED_LINKS_MATRIX_DIR = "joined-item-links-matrix";
        private static final String DEFAULT_SOLR_ITEM_LINKS_DOCS_DIR = "item-links-docs";
        private static final String DEFAULT_A_HISTORY_FIELD_NAME = "a_history";
        private static final String DEFAULT_B_HISTORY_FIELD_NAME = "b_history";
        private static final String DEFAULT_SOLR_USER_HISTORY_DOCS_DIR = "user-history-docs";
        private static final String DEFAULT_TEMP_DIR = "tmp";
        private String bTransposeBMatrixDir;//required
        private String bTransposeAMatrixDir = "";//optional
        private String aUserHistoryMatrixDir;//required
        private String bUserHistoryMatrixDir;//required
        private String indexesDir;//required
        private String userIndexFilePath;
        private String itemIndexFilePath;
        private String outputDir;//required
        private String itemIdFieldName = DEFAULT_ITEM_ID_FIELD_NAME;
        private String userIdFieldName = DEFAULT_USER_ID_FIELD_NAME;
        private String bTranposeBFieldName = DEFAULT_B_TRANSPOSE_B_FIELD_NAME;
        private String bTransposeAFieldName = DEFAULT_B_TRANSPOSE_A_FIELD_NAME;
        private String bUserHistoryFieldName = DEFAULT_B_HISTORY_FIELD_NAME;
        private String aUserHistoryFieldName = DEFAULT_A_HISTORY_FIELD_NAME;
        private String solrItemLinksDocsDir;//derived from requied output dir
        private String solrItemsLinksDocFilePath;//derived from required stuff
        private String tempDir = DEFAULT_TEMP_DIR;//defaults to output/tmp
        private String solrUserHistoryDir;//derived from required stuff


        Options() {
        }

        public String getTempDir() {
            return tempDir;
        }

        public String getBUserHistoryFieldName() {
            return bUserHistoryFieldName;
        }

        public String getAUserHistoryFieldName() {
            return aUserHistoryFieldName;
        }

        public String getSolrUserHistoryDir() {
            return solrUserHistoryDir;
        }

        @Option(name = "-t", aliases = {"--tempDir"}, usage = "Directory for intermediate results (optional). Default: 'output/tmp'.", required = false)
        public void setTempDir(String tempDir) {
            this.tempDir = tempDir;
        }

        public String getBUserHistoryMatrixDir() {
            return bUserHistoryMatrixDir;
        }

        @Option(name = "-upm", aliases = {"--usersPrimaryHistoryDir"}, usage = "Input directory containing the Mahout DistributedRowMatrix with users' history of primary actions (optional).", required = true)
        public void setBUserHistoryMatrixDir(String bUserHistoryMatrixDir) {
            this.bUserHistoryMatrixDir = bUserHistoryMatrixDir;
        }

        public String getAUserHistoryMatrixDir() {
            return aUserHistoryMatrixDir;
        }

        @Option(name = "-usm", aliases = {"--usersSecondaryHistoryDir"}, usage = "Input directory containing the Mahout DistributedRowMatrix with users' history of secondary or 'cross' actions (optional).", required = false)
        public void setAUserHistoryMatrixDir(String aUserHistoryMatrixDir) {
            this.aUserHistoryMatrixDir = aUserHistoryMatrixDir;
        }

        public String getItemIdFieldName() {
            return itemIdFieldName;
        }

        public String getUserIdFieldName() {
            return userIdFieldName;
        }

        public String getBTranposeBFieldName() {
            return bTranposeBFieldName;
        }

        public String getBTransposeAFieldName() {
            return bTransposeAFieldName;
        }

        public String getBTransposeBMatrixDir() {
            return bTransposeBMatrixDir;
        }

        @Option(name = "-ism", aliases = {"--itemSimilarityMatrixDir"}, usage = "Input directory containing the Mahout DistributedRowMatrix with Item-Item similarities for the primary action.", required = true)
        public void setBTransposeBMatrixDir(String bTransposeBMatrixDir) {
            this.bTransposeBMatrixDir = bTransposeBMatrixDir;
        }

        public String getBTransposeAMatrixDir() {
            return bTransposeAMatrixDir;
        }

        @Option(name = "-icsm", aliases = {"--itemCrossSimilarityMatrixDir"}, usage = "Input directory containing the Mahout DistributedRowMatrix with Item-Item cross-action similarities.", required = false)
        public void setBTransposeAMatrixDir(String bTransposeAMatrixDir) {
            this.bTransposeAMatrixDir = bTransposeAMatrixDir;
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
            this.solrItemLinksDocsDir = new Path(new Path(this.outputDir), DEFAULT_SOLR_ITEM_LINKS_DOCS_DIR).toString();
            this.solrUserHistoryDir = new Path(new Path(this.outputDir), DEFAULT_SOLR_USER_HISTORY_DOCS_DIR).toString();

        }

        public String getSolrItemLinksDocsDir() {
            return solrItemLinksDocsDir;
        }

        public String getSolrItemsLinksDocFilePath() {
            return solrItemsLinksDocFilePath;
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
