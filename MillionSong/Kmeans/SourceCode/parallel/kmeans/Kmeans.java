package kmeans;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Kmeans {

	public static int run(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: Kmeans <in> <out> <k>");
			System.exit(3);
		}
		
		conf.set("pointsPath", args[0]);
		conf.set("clustersPath", args[1] + "/curClusters");
		conf.set("updatedClustersPath", args[1] + "/updatedClusters");
		conf.set("clustersNum", args[2]);
		int reduceNum = Integer.parseInt(args[2]);
		
		Utils.initialClusters(conf);
		
		int code = 1;
		while (!Utils.isConverge(conf)) {
			Utils.updateCenters(conf);
			String outUri = args[1] + "/updatedClusters";
			FileSystem fs = FileSystem.get(URI.create(outUri), conf);
			Path out = new Path(outUri);
			// if output folder exists, remove it so next iteration can be
			// executed
			if (fs.exists(out)) {
				fs.delete(out, true);
			}
			
			Job job = new Job(conf, "Kmeans");
			job.setJarByClass(Kmeans.class);
			job.setMapperClass(KmeansJob.kmeansMapper.class);
			job.setReducerClass(KmeansJob.kmeansReducer.class);
			job.setNumReduceTasks(reduceNum);
			
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[1] + "/updatedClusters"));
			code = job.waitForCompletion(true) ? 0 : 1;
		}

		return code;
	}

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		int exitCode = run(args);
		System.exit(exitCode);
	}

}
