package storm;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.DefaultVertex;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;

import java.io.*;

/**
 * Created by faisal on 1/5/16.
 */
public class WeightedEdgeReader {
    String inputPath=null;
    FileInputStream fis = null;
    BufferedReader reader = null;

    public WeightedEdgeReader(String inputPath){
        try {
            fis = new FileInputStream(inputPath);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        reader = new BufferedReader(new InputStreamReader(fis));

        System.out.println("Reading File line by line using BufferedReader");
    }

    public Vertex<LongWritable,LongWritable,LongWritable> nextVertexEdge(){
        String line = null;
        Vertex<LongWritable,LongWritable,LongWritable> v = new DefaultVertex<LongWritable,LongWritable,LongWritable>();
        try {
            line = reader.readLine();
            String[] vals = line.split(",");
            LongWritable vertexId = new LongWritable(Long.parseLong(vals[0]));
            LongWritable vertexValue = new LongWritable(Long.parseLong(vals[1]));
            LongWritable edgeValue = new LongWritable(Long.parseLong(vals[2]));
            EdgeFactory.create(new LongWritable(Long.valueOf(0)),edgeValue);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
