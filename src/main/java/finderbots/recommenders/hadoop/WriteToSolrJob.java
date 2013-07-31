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
 * <p>Writes the DRMs passed in to Solr. The Primary DRM is expected to be a item-item similarity matrix with Mahout internal ID. The Secondary DRM is from cross-similarities. It also needs a file containing a map of internal mahout IDs to external IDs--one for userIDs and one for itemIDs. It needs the location to put the similarity matrixes, each will be put into a Solr fields of type 'string' for indexing.</p>
 *
 */

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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


        cleanOutputDirs(options);

        return 0;
    }

    private void cleanOutputDirs(Options options) throws IOException {
        FileSystem fs = FileSystem.get(getConf());
        //instead of deleting all, delete only the ones we overwrite
        //Path primaryOutputDir = new Path(options.getPrimaryOutputDir());
        //try {
        //    fs.delete(primaryOutputDir, true);
        //} catch (Exception e) {
        //    LOGGER.info("No primary output dir to delete, skipping.");
        //}
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new WriteToSolrJob(), args);
    }

    // Command line options for this job. Execute the main method above with no parameters
    // to get a help listing.
    //

    public class Options {

        private static final String DEFAULT_ID_INDEXES_DIR = ActionSplitterJob.Options.DEFAULT_INDEX_DIR;
        private static final String DEFAULT_ITEM_INDEX_FILENAME = ActionSplitterJob.Options.DEFAULT_ITEM_INDEX_FILENAME;
        private static final String DEFAULT_USER_INDEX_FILENAME = ActionSplitterJob.Options.DEFAULT_USER_INDEX_FILENAME;
        private String itemSimilarityMatrixDir;//required
        private String crossSimilarityMatrixDir = "";//optional
        private String indexesDir = DEFAULT_ID_INDEXES_DIR;
        private String userIndexFilePath;
        private String itemIndexFilePath;
        private String outputDir;//required

        Options() {
            userIndexFilePath = new Path(indexesDir, DEFAULT_USER_INDEX_FILENAME).toString();
            itemIndexFilePath = new Path(indexesDir, DEFAULT_ITEM_INDEX_FILENAME).toString();
        }

        public String getItemSimilarityMatrixDir() {
            return itemSimilarityMatrixDir;
        }

        @Option(name = "-ism", aliases = {"--itemSimilarityMatrixDir"}, usage = "Input directory containing the Mahout DistributedRowMatrix containing Item-Item similarities. Will be written to Solr.", required = true)
        public void setItemSimilarityMatrixDir(String itemSimilarityMatrixDir) {
            this.itemSimilarityMatrixDir = itemSimilarityMatrixDir;
        }

        public String getIndexesDir() {
            return indexesDir;
        }

        @Option(name = "-ix", aliases = {"--indexDir"}, usage = "Where to put user and item indexes (optional). Default: 'id-indexes'", required = false)
        public void setIndexesDir(String indexesDir) {
            this.indexesDir = indexesDir;
        }

        public String getCrossSimilarityMatrixDir() {
            return crossSimilarityMatrixDir;
        }

        @Option(name = "-csm", aliases = {"--crossSimilarityMatrixDir"}, usage = "Input directory containing the Mahout DistributedRowMatrix containing Item-Item cross-action similarities. Will be written to Solr.", required = true)
        public void setCrossSimilarityMatrixDir(String crossSimilarityMatrixDir) {
            this.crossSimilarityMatrixDir = crossSimilarityMatrixDir;
        }

        public String getUserIndexFilePath() {
            return userIndexFilePath;
        }

        @Option(name = "-uix", aliases = {"--userIndex"}, usage = "Input directory containing the serialized BiMap of Mahout ID <-> external ID.", required = true)
        public void setUserIndexFilePath(String userIndexFilePath) {
            this.userIndexFilePath = userIndexFilePath;
        }

        public String getItemIndexFilePath() {
            return itemIndexFilePath;
        }

        @Option(name = "-iix", aliases = {"--itemIndex"}, usage = "Input directory containing the serialized BiMap of Mahout ID <-> external ID.", required = true)
        public void setItemIndexFilePath(String itemIndexFilePath) {
            this.itemIndexFilePath = itemIndexFilePath;
        }

        public String getOutputDir() {
            return outputDir;
        }

        @Option(name = "-o", aliases = {"--output"}, usage = "Where to write docs of ids for indexing.", required = true)
        public void setOutputDir(String outputDir) {
            this.outputDir = outputDir;
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
