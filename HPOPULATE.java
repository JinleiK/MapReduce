package HPOPULATE;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import au.com.bytecode.opencsv.CSVParser;

public class HPOPULATE {
	
	private static String tablename = "AIRLINE_DELAY";
	private static String delayFamily = "DELAYMINUTES";
	
	public static class PopulateMapper extends 
			Mapper<LongWritable, Text, ImmutableBytesWritable, DoubleWritable>{
		private ImmutableBytesWritable keyOut = new ImmutableBytesWritable();
		private DoubleWritable valueOut = new DoubleWritable();
		@Override
		public void map(LongWritable keyin, Text value, Context context) 
				throws IOException, InterruptedException{
			//retrieve the needed value from the current line
			CSVParser parser = new CSVParser();
			String nextLine[] = parser.parseLine(value.toString());
			String year = nextLine[0];
			String month = nextLine[2];
			String airlineId = nextLine[7];
			double arrDelayMinutes;
			String isCancelled = nextLine[41];
			String isDiverted = nextLine[43];
			String isDeparted;
			
			if(month.length() == 1){
				month = "0" + month;
			}
			
			if(isCancelled.equals("0.00") & isDiverted.equals("0.00")){
				isDeparted = "0";
				arrDelayMinutes = Double.parseDouble(nextLine[37]);
			} else {
				isDeparted = "1";
				arrDelayMinutes = 0.00;
			}
			//generate rowkey
			String rowString = year + isDeparted + airlineId + month;
			byte[] rowkey = rowString.getBytes();
			keyOut.set(rowkey);
			valueOut.set(arrDelayMinutes);
			context.write(keyOut, valueOut);
		}		
	}
	
	public static class PopulateReducer extends 
			TableReducer<ImmutableBytesWritable, DoubleWritable, ImmutableBytesWritable>{
		@Override
		public void reduce(ImmutableBytesWritable keyin, 
				Iterable<DoubleWritable> values, Context context) 
				throws IOException, InterruptedException{
			int count = 1;
			for(DoubleWritable value : values){
				byte[] rowKey = rowkeyConverter(keyin.get(), count);
				Put put = new Put(rowKey);
				put.add(
						Bytes.toBytes(delayFamily), 
						Bytes.toBytes("delay"), 
						Bytes.toBytes(value.get()));
				context.write(new ImmutableBytesWritable(rowKey), put);
				count ++;
			}
		}
		
		protected byte[] rowkeyConverter(byte[] prefix, int count){
			byte[] rowkey = new byte[prefix.length + Bytes.SIZEOF_INT];
			Bytes.putBytes(rowkey, 0, prefix, 0, prefix.length);
			Bytes.putInt(rowkey, prefix.length, count);
			return rowkey;
		}
	}
	
	public static int run(String[] args) 
			throws IOException, InterruptedException, ClassNotFoundException{
		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 1) {
			System.err.println("Usage: populate <in>");
			System.exit(1);
		}
		//create table
		HBaseAdmin admin = new HBaseAdmin(conf);
		HTableDescriptor htd = new HTableDescriptor(tablename);
		HColumnDescriptor hcd = new HColumnDescriptor(delayFamily); 
		htd.addFamily(hcd);
		//if table exists, drop it
		if(admin.tableExists(tablename)){
			admin.disableTable(tablename);
			admin.deleteTable(tablename);
		}
		admin.createTable(htd);
		
		conf.set(TableOutputFormat.OUTPUT_TABLE, tablename);
		Job job = new Job(conf, "populate htable");
		job.setJarByClass(HPOPULATE.class);
		job.setMapperClass(PopulateMapper.class);
		job.setNumReduceTasks(10); 
		job.setReducerClass(PopulateReducer.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputFormatClass(TableOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		int exitCode = job.waitForCompletion(true) ? 0 : 1;
		admin.close();
		return exitCode;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = run(args);
		System.exit(exitCode);
	}
}
