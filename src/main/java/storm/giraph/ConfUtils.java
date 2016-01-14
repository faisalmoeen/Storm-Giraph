package storm.giraph;

import org.apache.giraph.bsp.BspInputFormat;
import org.apache.giraph.bsp.BspOutputFormat;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.GraphMapper;
import org.apache.giraph.job.GiraphJob;
import org.apache.giraph.job.GiraphJobRetryChecker;
import org.apache.giraph.job.JobProgressTrackerService;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * Created by faisal on 1/9/16.
 */
public class ConfUtils {
    public static Job getHadoopConf(GiraphJob giraphJob){
        // Set the job properties, check them, and submit the job
        ImmutableClassesGiraphConfiguration conf =
                new ImmutableClassesGiraphConfiguration(giraphJob.getConfiguration());
//        checkLocalJobRunnerConfiguration(conf);

        int tryCount = 0;
        GiraphJobRetryChecker retryChecker = conf.getJobRetryChecker();
        while (true) {
            JobProgressTrackerService jobProgressTrackerService =
                    JobProgressTrackerService.createJobProgressServer(conf);

            tryCount++;
            Job submittedJob = null;
            try {
                submittedJob = new Job(conf, giraphJob.getJobName());
            } catch (IOException e) {
                e.printStackTrace();
            }
            submittedJob.setNumReduceTasks(0);
            submittedJob.setMapperClass(GraphMapper.class);
            submittedJob.setInputFormatClass(BspInputFormat.class);
            submittedJob.setOutputFormatClass(BspOutputFormat.class);
            return submittedJob;
        }
    }
}
