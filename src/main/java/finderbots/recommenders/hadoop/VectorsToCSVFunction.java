package finderbots.recommenders.hadoop;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

/**
 * User: pat
 * Date: 8/12/13
 * Time: 4:45 PM
 */

public class VectorsToCSVFunction extends BaseOperation implements Function {
    private static Logger LOGGER = Logger.getRootLogger();
    private static HashBiMap<String,String> itemIndex;
    private static HashBiMap<String,String> rowIndex;

    static class Context{
        public static int i;
    }

    public VectorsToCSVFunction(Fields outfields){
        super(outfields);
    }

    public void operate( FlowProcess flowProcess, FunctionCall functionCall )
    {
        // get the current argument values
        TupleEntry arguments = functionCall.getArguments();
        // get the context for this grouping
        // update the context object
        int key = arguments.getInteger(WriteDRMsToSolr.getiDFieldName());
        int keyB = arguments.getInteger(WriteDRMsToSolr.getiDFieldName());
        try {
            String doJoinString = (String)flowProcess.getProperty("joining");
            String itemIDString = rowIndex.inverse().get(String.valueOf(key));
            Vector va = ((VectorWritable)arguments.getObject(WriteDRMsToSolr.getdRM1FieldName())).get();
            String vaDoc = createOrderedDoc(va, itemIndex);
            Tuple tuple;
            if(doJoinString.equals("true")){
                Vector vb = ((VectorWritable)arguments.getObject(WriteDRMsToSolr.getDRM2FieldName())).get();
                String vbDoc = createOrderedDoc(vb, itemIndex);
                tuple = new Tuple(itemIDString, vaDoc, vbDoc);
            } else { // not joining, just converting to CSV
                tuple = new Tuple(itemIDString, vaDoc);
            }
            functionCall.getOutputCollector().add(tuple);

        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        //now do some magic to write fields to the output tuple.
        int i = 0;
    }

    @Override
    public void prepare(cascading.flow.FlowProcess flowProcess, cascading.operation.OperationCall operationCall) {
        try {
            String itemIndexPath = (String)flowProcess.getProperty("itemIndexPath");
            String rowIndexPath = (String)flowProcess.getProperty("rowIndexPath");
            itemIndex = Utils.readIndex(new Path(itemIndexPath));
            if(!itemIndexPath.equals(rowIndexPath)){
                rowIndex = Utils.readIndex(new Path(rowIndexPath));
            } else { //identical indexes
                rowIndex = itemIndex;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String createOrderedDoc( Vector v, BiMap<String, String> elementIndex){
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


    String createDoc(Vector v, HashBiMap<String,String> index){
        String doc = "";
        for(Vector.Element ve : v.nonZeroes()){
            doc += index.inverse().get(String.valueOf(ve.index()))+" ";
        }
        return doc;
    }


}
