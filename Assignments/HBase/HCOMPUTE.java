package HCOMPUTE;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class HCOMPUTE {
	
	private static String tablename = "AIRLINE_DELAY";
	private static String delayFamily = "DELAYMINUTES";
	private static byte[] DELAYFAMILY = Bytes.toBytes(delayFamily);
	private static byte[] DELAYCOLUMN = Bytes.toBytes("delay");
	private static byte[] STARTROWKEY = Bytes.toBytes("20080");
	private static byte[] ENDROWKEY = Bytes.toBytes("20081");
	
	public static class ComputeMapper extends TableMapper<Text, Text>{
		private Text keyOut = new Text();
		private Text valOut = new Text();
		@Override
		public void map(ImmutableBytesWritable rowkey, Result values, Context context) 
				throws IOException, InterruptedException{
			String airlineID = Bytes.toString(rowkey.get()).substring(5, 10);
			String month = Bytes.toString(rowkey.get()).substring(10, 12);
			//get rid of "0" in front of month
			if(month.startsWith("0")){
				month = month.substring(1);
			}
			Double value = Bytes.toDouble(values.getValue(DELAYFAMILY, DELAYCOLUMN));
			keyOut.set(airlineID);
			valOut.set(month + ":" + value);
			context.write(keyOut, valOut);
		}
	}
	
	public static class ComputeReducer extends Reducer<Text, Text, NullWritable, Text>{
		private NullWritable nullkey = NullWritable.get();
		@Override
		public void reduce(Text keyin, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException{
			String resultString = keyin.toString();
			double sum[] = new double[12];
			Arrays.fill(sum, 0.00);
			int count[] = new int[12];
			double average[] = new double[12];
			int month;
			String line[];
			//caculate the sum and count of the delayminutes of each month
			for(Text val: values){
				line = val.toString().split(":");
				month = Integer.parseInt(line[0]);
				sum[month-1] += Double.parseDouble(line[1]);
				count[month-1] ++;
			}
			//calculate the average delay of each month, 
			//if there's no record in one month, set the average to NULL
			for(int i = 0; i < 12; i++){
				if (count[i] != 0){
					average[i] = sum[i]/count[i];
					resultString += ", (" + (i+1) + "," 
									+ (int) Math.round(average[i]) + ")";
				} else{
					resultString += ", (" + (i+1) + ",NULL)";
				}
			}
			context.write(nullkey, new Text(resultString));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 1) {
			System.err.println("Usage: compute <out>");
			System.exit(1);
		}
		//set the scan with startRowkey and endRowkey
		Scan scan = new Scan(STARTROWKEY, ENDROWKEY);
		scan.setCaching(1000);
		scan.setCacheBlocks(false);
		Job job = new Job(conf, "compute htable");

		job.setJarByClass(HCOMPUTE.class);
		TableMapReduceUtil.initTableMapperJob(tablename, scan,
				ComputeMapper.class, Text.class, Text.class, job);
		job.setNumReduceTasks(10);
		job.setReducerClass(ComputeReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
		int exitCode = job.waitForCompletion(true) ? 0 : 1;
		System.exit(exitCode);
	}
}
