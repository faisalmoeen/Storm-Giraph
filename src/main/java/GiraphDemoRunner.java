import org.apache.commons.io.FileUtils;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.examples.SimpleShortestPathsComputation;
import org.apache.giraph.io.formats.GiraphFileInputFormat;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.io.formats.JsonLongDoubleFloatDoubleVertexInputFormat;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.IOException;

public class GiraphDemoRunner implements Tool{

	private Configuration conf;
	private String inputPath;
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
		try {
			FileUtils.deleteDirectory(new File(outputPath));
		}catch (IOException e){
			e.printStackTrace();
		}
		this.outputPath = outputPath;
	}

	private String outputPath;
	
	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public int run(String[] arg0) throws Exception {
		
		setInputPath("/home/faisal/git/Storm-Giraph/src/main/resources/tiny_graph.txt");
		setOutputPath("/tmp/graph_out");
		GiraphConfiguration giraphConf = new GiraphConfiguration(getConf());
		
		giraphConf.setComputationClass(SimpleShortestPathsComputation.class);
		
		giraphConf.setVertexInputFormatClass(JsonLongDoubleFloatDoubleVertexInputFormat.class);
		GiraphFileInputFormat.addVertexInputPath(giraphConf, new Path(getInputPath()));
		giraphConf.setVertexOutputFormatClass(IdWithValueTextOutputFormat.class);
		giraphConf.setLocalTestMode(true);
		giraphConf.setWorkerConfiguration(1, 1, 100);
		giraphConf.setMaxNumberOfSupersteps(100);
		giraphConf.SPLIT_MASTER_WORKER.set(giraphConf, false);
		giraphConf.USE_OUT_OF_CORE_GRAPH.set(giraphConf, true);
		
		GiraphJob giraphJob = new GiraphJob(giraphConf,"GiraphDemo");
		FileOutputFormat.setOutputPath(giraphJob.getInternalJob(), new Path(getOutputPath()));
		
		giraphJob.run(true);
		return 0;
	}

	public static void main(String[] args) throws Exception{
		ToolRunner.run(new GiraphDemoRunner(), args);
	}
}
