package storm.giraph;

import org.apache.giraph.bsp.BspInputFormat;
import org.apache.giraph.bsp.BspOutputFormat;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.examples.SimpleShortestPathsComputation;
import org.apache.giraph.graph.GraphMapper;
import org.apache.giraph.io.formats.GiraphFileInputFormat;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.io.formats.JsonLongDoubleFloatDoubleVertexInputFormat;
import org.apache.giraph.job.GiraphJob;
import org.apache.giraph.job.GiraphJobRetryChecker;
import org.apache.giraph.job.JobProgressTrackerService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by faisal on 1/10/16.
 */
public class StormContext extends org.apache.hadoop.mapreduce.MapContext {

    public StormContext(Configuration conf, TaskAttemptID taskid, RecordReader reader, RecordWriter writer, OutputCommitter committer, StatusReporter reporter, InputSplit split) {
        super(conf, taskid, reader, writer, committer, reporter, split);
    }

//    public StormContext(Configuration conf, TaskAttemptID taskid, RecordWriter output, OutputCommitter committer, StatusReporter reporter) {
//        super(conf, taskid, output, committer, reporter);
//    }

    public StormContext(Configuration conf,TaskAttemptID taskid){
        super(conf,taskid);
    }

    public void setStatus(String status) {
        //TODO: reporter.setStatus(status)
    }

    public Configuration getConfiguration() {
        String inputPath = "/home/faisal/git/Storm-Giraph/src/main/resources/tiny_graph.txt";
        String outputPath = "/tmp/graph_out";
        GiraphConfiguration giraphConf = new GiraphConfiguration(new Configuration());

        giraphConf.setComputationClass(SimpleShortestPathsComputation.class);

        giraphConf.setVertexInputFormatClass(JsonLongDoubleFloatDoubleVertexInputFormat.class);
        try {
            GiraphFileInputFormat.addVertexInputPath(giraphConf, new Path(inputPath));
        } catch (IOException e) {
            e.printStackTrace();
        }
        giraphConf.setVertexOutputFormatClass(IdWithValueTextOutputFormat.class);
        giraphConf.setLocalTestMode(true);
        giraphConf.setWorkerConfiguration(1, 1, 100);
        giraphConf.setMaxNumberOfSupersteps(100);
        giraphConf.SPLIT_MASTER_WORKER.set(giraphConf, false);
        giraphConf.USE_OUT_OF_CORE_GRAPH.set(giraphConf, true);
        giraphConf.set("giraph.zkList", "localhost:2181");

        GiraphJob giraphJob = null;
        try {
            giraphJob = new GiraphJob(giraphConf, "GiraphDemo");
        } catch (IOException e) {
            e.printStackTrace();
        }
        FileOutputFormat.setOutputPath(giraphJob.getInternalJob(), new Path(outputPath));
        ImmutableClassesGiraphConfiguration conf =
                new ImmutableClassesGiraphConfiguration(giraphConf);
//        checkLocalJobRunnerConfiguration(conf);

        int tryCount = 0;
        GiraphJobRetryChecker retryChecker = conf.getJobRetryChecker();
        while (true) {
            JobProgressTrackerService jobProgressTrackerService =
                    JobProgressTrackerService.createJobProgressServer(conf);

            tryCount++;
            Job submittedJob = null;
            try {
                submittedJob = new Job(conf, "GiraphDemo2");
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (submittedJob.getJar() == null) {
                submittedJob.setJarByClass(getClass());
            }
            submittedJob.setNumReduceTasks(0);
            submittedJob.setMapperClass(GraphMapper.class);
            submittedJob.setInputFormatClass(BspInputFormat.class);
            submittedJob.setOutputFormatClass(BspOutputFormat.class);
            return submittedJob.getConfiguration();
        }

    }

    public void progress() {
        //TODO: implement method
    }
}
