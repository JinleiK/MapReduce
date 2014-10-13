package kmeans;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class KmeansJob {
	public static class kmeansMapper extends
			Mapper<Object, Text, IntWritable, Text> {

		private String[] currCenters;
		private int num;
		private IntWritable keyOut = new IntWritable();
		private Text valueOut = new Text();

		@Override
		public void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			String folder = conf.get("clustersPath");
			num = Integer.parseInt(conf.get("clustersNum"));
			currCenters = Utils.readCenters(folder, num, conf);
		}

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String point = value.toString();
			double min = Double.MAX_VALUE;
			double dist;
			for (int i = 0; i < num; i++) {
				dist = Utils.distance(currCenters[i], point);
				if (dist < min) {
					min = dist;
					keyOut.set(i);
				}
			}
			valueOut.set(point);
			context.write(keyOut, valueOut);
		}
	}

	public static class kmeansReducer extends
			Reducer<IntWritable, Text, NullWritable, Text> {
		private NullWritable outKey = NullWritable.get();

		public void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			double[] sum = new double[Utils.DIMENSION];
			double count = 0.0;
			double[] point = new double[Utils.DIMENSION];
			double[] newCenter = new double[Utils.DIMENSION];
			for (Text v : values) {
				point = Utils.stringToDoubles(v.toString());
				for (int i = 0; i < Utils.DIMENSION; i++) {
					sum[i] += point[i];
				}
				count++;
			}
			for (int i = 0; i < Utils.DIMENSION; i++) {
				newCenter[i] = sum[i] / count;
			}
			String outString = Utils.doublesToString(newCenter);
			// updatedCenters[key.get()] = outString;
			context.write(outKey, new Text(outString));
		}
	}
}
