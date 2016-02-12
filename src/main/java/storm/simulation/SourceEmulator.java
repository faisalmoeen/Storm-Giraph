package storm.simulation;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.google.common.collect.Lists;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.JsonLongDoubleFloatDoubleVertexInputFormat;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.json.JSONArray;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.WeightedEdgeReader;
import storm.giraph.ContextConstructor;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by faisal on 1/4/16.
 */
public class SourceEmulator{
    public static Logger LOG = LoggerFactory.getLogger(SourceEmulator.class);
    boolean _isDistributed;
    SpoutOutputCollector _collector;
    String inputPath;
    WeightedEdgeReader edgeReader;
    JsonLongDoubleFloatDoubleVertexReader reader;
    BufferedReader br;
    String line;
    TopologyContext _context;
    Map _conf;
    Mapper.Context mapContext = null;
    Integer taskId;
    JSONArray jsonArray;
    /** Class logger */

    public SourceEmulator() {
        this(true);
    }

    public SourceEmulator(boolean isDistributed) {
        _isDistributed = isDistributed;
    }

    public void open(Map conf) {
        _conf = conf;

        taskId = _context.getThisTaskIndex();
        int attempt = 0;
        int jobId = 1;
        TaskAttemptID taskAttemptID = new TaskAttemptID("jobtracker",jobId,true,taskId,attempt);
        try {
            mapContext = (Mapper.Context) ContextConstructor.getMapContext(conf, taskAttemptID);
        } catch (Exception e) {
            e.printStackTrace();
        }
        assert mapContext != null;
        mapContext.getConfiguration().set("mapred.task.partition", taskId.toString());
        inputPath = (String)conf.get("inputPath");
        reader = new JsonLongDoubleFloatDoubleVertexReader();
//        edgeReader = new WeightedEdgeReader(inputPath);
        try {
            br = new BufferedReader(new FileReader(new File(inputPath)));

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void close() {
    }

    public void nextTuple() {
        try {
            if((line = br.readLine()) != null) {
                jsonArray = reader.preprocessLine(new Text(line));
                LongWritable id = reader.getId(jsonArray);
//                MasterPa
                _collector.emit("s2p", new Values(line));
            }
            else{

                Utils.sleep(999999999999l);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (JSONException e) {
            e.printStackTrace();
        }
        _collector.emit("s2p", new Values("1"));
        _collector.emit("s2p", new Values("2"));
        _collector.emit("s2p", new Values("3"));
    }

    public void ack(Object msgId) {

    }

    public void fail(Object msgId) {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("s2p",new Fields("partitionId"));
    }

    public Map<String, Object> getComponentConfiguration() {
        if(!_isDistributed) {
            Map<String, Object> ret = new HashMap<String, Object>();
            ret.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
            return ret;
        } else {
            return null;
        }
    }
}





class JsonLongDoubleFloatDoubleVertexReader{

    protected JSONArray preprocessLine(Text line) throws JSONException {
        return new JSONArray(line.toString());
    }

    protected LongWritable getId(JSONArray jsonVertex) throws JSONException,
            IOException {
        return new LongWritable(jsonVertex.getLong(0));
    }

    protected DoubleWritable getValue(JSONArray jsonVertex) throws
            JSONException, IOException {
        return new DoubleWritable(jsonVertex.getDouble(1));
    }

    protected Iterable<Edge<LongWritable, FloatWritable>> getEdges(
            JSONArray jsonVertex) throws JSONException, IOException {
        JSONArray jsonEdgeArray = jsonVertex.getJSONArray(2);
        List<Edge<LongWritable, FloatWritable>> edges =
                Lists.newArrayListWithCapacity(jsonEdgeArray.length());
        for (int i = 0; i < jsonEdgeArray.length(); ++i) {
            JSONArray jsonEdge = jsonEdgeArray.getJSONArray(i);
            edges.add(EdgeFactory.create(new LongWritable(jsonEdge.getLong(0)),
                    new FloatWritable((float) jsonEdge.getDouble(1))));
        }
        return edges;
    }

    protected Vertex<LongWritable, DoubleWritable, FloatWritable>
    handleException(Text line, JSONArray jsonVertex, JSONException e) {
        throw new IllegalArgumentException(
                "Couldn't get vertex from line " + line, e);
    }

}


