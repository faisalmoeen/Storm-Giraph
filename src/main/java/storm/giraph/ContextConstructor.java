package storm.giraph;

import backtype.storm.task.TopologyContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import java.util.Map;

/**
 * Created by faisal on 1/11/16.
 */
public class ContextConstructor{

    public static Configuration getHadoopCofiguration(Map conf) throws Exception{
        return GiraphTopology.getConfiguration();
    }

    public static MapContext getMapContext(Map conf, TaskAttemptID taskid) throws Exception {
        MapContext mapContext = null;
        try{
            mapContext = new Mapper().intantiateContext(getHadoopCofiguration(conf), taskid);
        }catch (Exception e){
            e.printStackTrace();
        }
        return mapContext;
    }
}
