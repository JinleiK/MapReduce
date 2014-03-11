import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import au.com.bytecode.opencsv.CSVParser;

public class SECONDARY {
	public static class myMapper extends Mapper<LongWritable, Text, IntPair, DoubleWritable>{
		private IntPair outputKey = new IntPair();
		private DoubleWritable outputValue = new DoubleWritable();
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException{
			//retrieve the needed value from the current line
			CSVParser parser = new CSVParser();
			String nextLine[] = parser.parseLine(value.toString());
			String year = nextLine[0];
			int month = Integer.parseInt(nextLine[2]);
			int airlineID = Integer.parseInt(nextLine[7]);
			double arrDelayMinutes;
			double isCancelled = Double.parseDouble(nextLine[41]);
			double isDiverted = Double.parseDouble(nextLine[43]);
			//filter out the records that actually departed in 2008
			if(year.equals("2008") && isCancelled == 0.00 && isDiverted == 0.00){
				arrDelayMinutes = Double.parseDouble(nextLine[37]);
				outputKey.setAirline(airlineID);
				outputKey.setMonth(month);
				outputValue.set(arrDelayMinutes);
				context.write(outputKey, outputValue);
			}
		}
	}
	
	public static class myKeyComparator extends WritableComparator{
		protected myKeyComparator(){
			super(IntPair.class,true);
		}
		@Override
		public int compare(WritableComparable tp1, WritableComparable tp2){
			IntPair ip1 = (IntPair) tp1;
			IntPair ip2 = (IntPair) tp2;
			int cmp = ip1.compareTo(ip2);
			if(cmp !=0){
				return cmp;
			}
			return ip1.monthCompare(ip2);
		}
	}
	
	public static class myPartitioner extends Partitioner<IntPair, DoubleWritable>{

		@Override
		public int getPartition(IntPair key, DoubleWritable value, int numPartitions) {
			return Math.abs(key.getAirline() *127) % numPartitions;
		}
	}
	
	public static class myGroupingComparator extends WritableComparator{
		public myGroupingComparator(){
			super(IntPair.class, true);
		}
		public int compare(WritableComparable tp1,WritableComparable tp2){
			IntPair ip1 = (IntPair) tp1;
			IntPair ip2 = (IntPair) tp2;
			return ip1.compareTo(ip2);
		}
	}
	
	public static class myReducer extends Reducer<IntPair, DoubleWritable, NullWritable, Text>{
		private NullWritable nullkey = NullWritable.get();
		
		public void reduce(IntPair key, Iterable<DoubleWritable> values, Context context) 
				throws IOException, InterruptedException{
			String outString = "";
			//get the first value
			Iterator<DoubleWritable> it = values.iterator();
			Double nextValue = it.next().get();
			int curComKey = key.getMonth();
			int nextComKey = key.getMonth();
			double sum = nextValue;
			int count = 1;
			int average;
			while(it.hasNext()){
				nextValue = it.next().get();
				nextComKey = key.getMonth();
				//if the new composite equals to the current composite key, 
				//add the value to sum, and add count
				if(nextComKey == curComKey){
					sum += nextValue;
					count ++;
				//else calculate the current average and
				//let the sum = the value, and set the count to 1
				} else {
					average = (int)Math.round(sum / count);
					outString += ", (" + curComKey + "," + average + ")";
					sum = nextValue;
					count = 1;
					curComKey = nextComKey;
				}
			}
			//calculate the average delay of the last month
			average = (int)Math.round(sum / count);
			outString += ", (" + curComKey + "," + average + ")";
			outString = key.getAirline() + outString;
			while(curComKey < 12){
				outString += ",(" + (++ curComKey) + ",NULL)";
			}
			context.write(nullkey, new Text(outString));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 2) {
	      System.err.println("Usage: Monthly Delay <in> <out>");
	      System.exit(2);
	    }
	    Job job = new Job(conf, "Monthly Delay");
	    job.setNumReduceTasks(10);
	    job.setPartitionerClass(myPartitioner.class);
	    job.setSortComparatorClass(myKeyComparator.class);
	    job.setGroupingComparatorClass(myGroupingComparator.class);
	    job.setJarByClass(SECONDARY.class);
	    job.setMapperClass(myMapper.class);
	    job.setReducerClass(myReducer.class);
	    job.setOutputKeyClass(IntPair.class);
	    job.setOutputValueClass(DoubleWritable.class);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
