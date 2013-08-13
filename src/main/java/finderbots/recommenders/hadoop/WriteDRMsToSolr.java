package finderbots.recommenders.hadoop;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.joiner.InnerJoin;
import cascading.scheme.hadoop.TextDelimited;
import cascading.scheme.hadoop.WritableSequenceFile;
import cascading.tap.MultiSourceTap;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

/**
 * User: pat
 * Date: 8/8/13
 * Time: 12:36 PM
 */

/**
 * This class joins two DRMs by their row ids. It each joined data set to a CSV with field names passed in
 * Examples: [B'B] and [B'A] similarity matrices joined by row id, which = item ids. B and A are joined by
 * row id, which = user ids.
 *
 * The constructor set up the fields and field names used by Cascading and for column headers in the CSVs
 * The main entry point is joinDRMsWriteToSolr(Path iDIndexPath, Path itemIndexPath, Path dRM1InputPath , Path dRM2InputPath, Path groupedCSVOutputPath)
 * the indexes may be identical.
 */

public class WriteDRMsToSolr {
    private static Logger LOGGER = Logger.getRootLogger();

    FileSystem fs;
    static String iDFieldName;
    String iD2FieldName;
    static String dRM1FieldName;
    static String dRM2FieldName;

    Fields inFieldsDRM1;
    Fields inFieldsDRM2;
    Fields common;
    Fields grouped;
    Fields joinedOutFields;
    Fields simpleOutFields;
/* example fields passed in:
        fields.put("iD1", options.getItemIdFieldName());
        fields.put("dRM1FieldName", options.getBTranposeBFieldName());
        fields.put("dRM2FieldName", options.getBTransposeAFieldName());

 */
    WriteDRMsToSolr(Map<String, String> fields) throws IOException {
        Configuration conf = new JobConf();
        fs = FileSystem.get(conf);
        iDFieldName = fields.get("iD1");
        dRM1FieldName = fields.get("dRM1FieldName");
        inFieldsDRM1 = new Fields(iDFieldName, dRM1FieldName);
        simpleOutFields = new Fields(iDFieldName, dRM1FieldName);
        if(fields.containsKey("dRM2FieldName")){//joining DRMs so defined needed fields
            iD2FieldName = iDFieldName+"2";//just to uniqueify it from the other id field name
            dRM2FieldName = fields.get("dRM2FieldName");
            inFieldsDRM2 = new Fields(iDFieldName, dRM2FieldName);
            common = new Fields(iDFieldName);
            grouped = new Fields(iDFieldName, dRM1FieldName, iD2FieldName, dRM2FieldName);
            joinedOutFields = new Fields(iDFieldName, dRM1FieldName, dRM2FieldName);
        }
    }

    void joinDRMsWriteToSolr(Path iDIndexPath, Path itemIndexPath, Path dRM1InputPath , Path dRM2InputPath, Path groupedCSVOutputPath) throws IOException {
        MultiSourceTap dRM1Source = getTaps(dRM1InputPath, inFieldsDRM1);
        MultiSourceTap dRM2Source = getTaps(dRM2InputPath, inFieldsDRM2);

        Pipe lhs = new Pipe("DRM1");
        Pipe rhs = new Pipe("DRM2");
        Pipe groupByItemIDPipe = new CoGroup(lhs, common, rhs, common, grouped, new InnerJoin());
        groupByItemIDPipe = new Each(groupByItemIDPipe, new VectorsToCSVFunction(joinedOutFields));
        //the DRMs (Mahout Distributed Row Matrices) have row and items indexes the two dictionary BiHashMaps
        //pass these to the output function so the strings from the indexes can be written instead of the
        //binary values of the Keys and Vectors in the DRMs
        groupByItemIDPipe.getStepConfigDef().setProperty("itemIndexPath", itemIndexPath.toString());
        // for these matrices the group by key is the id from the Mahout row key
        groupByItemIDPipe.getStepConfigDef().setProperty("rowIndexPath", iDIndexPath.toString());
        groupByItemIDPipe.getStepConfigDef().setProperty("joining", "true");

        Tap groupedOutputSink = new Hfs(new TextDelimited(true,","), groupedCSVOutputPath.toString());

        FlowDef flowDef = new FlowDef()
            .setName("group-DRMs-by-key")
            .addSource(lhs, dRM1Source)
            .addSource(rhs, dRM2Source)
            .addTailSink(groupByItemIDPipe, groupedOutputSink);
        Flow flow = new HadoopFlowConnector().connect(flowDef);
        flow.complete();

        //todo: not sure if it matters but may need to rename the part files to .csv
    }

    void writeDRMToSolr(Path iDIndexPath, Path itemIndexPath, Path dRM1InputPath, Path cSVOutputPath) throws IOException {
        MultiSourceTap dRM1Source = getTaps(dRM1InputPath, inFieldsDRM1);

        Pipe dRM1 = new Pipe("DRM1");
        dRM1 = new Each(dRM1, new VectorsToCSVFunction(simpleOutFields));
        //the DRM (Mahout Distributed Row Matrix) has row and items indexes the two dictionary BiHashMaps
        //pass these to the output function so the strings from the indexes can be written instead of the
        //binary values of the Keys and Vectors in the DRMs
        dRM1.getStepConfigDef().setProperty("itemIndexPath", itemIndexPath.toString());
        dRM1.getStepConfigDef().setProperty("rowIndexPath", iDIndexPath.toString());
        dRM1.getStepConfigDef().setProperty("joining", "false");
        Tap outputSink = new Hfs(new TextDelimited(true,","), cSVOutputPath.toString());

        FlowDef flowDef = new FlowDef()
            .setName("convert-to-CSV")
            .addSource(dRM1, dRM1Source)
            .addTailSink(dRM1, outputSink);
        Flow flow = new HadoopFlowConnector().connect(flowDef);
        flow.complete();

        //todo: not sure if it matters but may need to rename the part files to .csv
    }

    MultiSourceTap getTaps(Path p, Fields f) throws IOException {

        FileStatus[] stats = fs.listStatus(p);
        ArrayList<Tap> all = new ArrayList<Tap>();
        if(stats != null){
            for( FileStatus s : stats ){
                if(s.getPath().toString().contains("part-")){//found a part-xxxxx file
                    Path filePath = new Path(s.getPath().toString());
                    //Tap t = new Hfs( inFields, filePath.toString());
                    //Tap t = new Hfs(new TextLine(), filePath.toString(), true);
                    Tap t = new Hfs( new WritableSequenceFile( f, LongWritable.class, VectorWritable.class ), filePath.toString() );
                    if( s.getLen() != 0 ){// then part file is not empty
                        all.add(t);
                    }
                }
            }
        }
        Tap[] sourceTaps = all.toArray(new Tap[all.size()]);
        if(sourceTaps.length == 0 ) throw new IOException("No Source files found");
        MultiSourceTap source = new MultiSourceTap(sourceTaps);

        return source;
    }

    public static String getiDFieldName() {
        return iDFieldName;
    }

    public static String getdRM1FieldName() {
        return dRM1FieldName;
    }

    public static String getDRM2FieldName() {
        return dRM2FieldName;
    }
}
