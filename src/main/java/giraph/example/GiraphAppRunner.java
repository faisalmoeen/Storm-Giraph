package giraph.example;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.io.formats.GiraphFileInputFormat;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.io.formats.JsonLongDoubleFloatDoubleVertexInputFormat;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by faisal on 8/14/15.
 */
public class GiraphAppRunner implements Tool{
    private Configuration conf;
    private String inputPath;
    private String outputPath;

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new GiraphAppRunner(), args);
    }

    public int run(String[] args) throws Exception {
        setInputPath("/tmp/tiny_graph.txt");
        setOutputPath("/tmp/giraphOutput");

        GiraphConfiguration giraphConfiguration = new GiraphConfiguration(getConf());

        giraphConfiguration.setComputationClass(SimpleShortestPathsComputation.class);
        giraphConfiguration.setVertexInputFormatClass(JsonLongDoubleFloatDoubleVertexInputFormat.class);
        GiraphFileInputFormat.addVertexInputPath(giraphConfiguration, new Path(getInputPath()));
        giraphConfiguration.setVertexOutputFormatClass(IdWithValueTextOutputFormat.class);

        giraphConfiguration.setWorkerConfiguration(0, 1, 100);
        giraphConfiguration.setLocalTestMode(true);
        giraphConfiguration.setMaxNumberOfSupersteps(100);

        giraphConfiguration.SPLIT_MASTER_WORKER.set(giraphConfiguration, false);
        giraphConfiguration.USE_OUT_OF_CORE_GRAPH.set(giraphConfiguration, true);

        GiraphJob giraphJob = new GiraphJob(giraphConfiguration, getClass().getName());
        FileOutputFormat.setOutputPath(giraphJob.getInternalJob(), new Path(getOutputPath()));
        giraphJob.run(true);
        return 1;
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    public Configuration getConf() {
        return conf;
    }

    public String getInputPath() {
        return inputPath;
    }

    public void setInputPath(String inputPath) {
        this.inputPath = inputPath;
    }

    public String getOutputPath() {
        return outputPath;
    }

    public void setOutputPath(String outputPath) {
        this.outputPath = outputPath;
    }
}
