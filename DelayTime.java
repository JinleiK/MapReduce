package AverageDelay;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import au.com.bytecode.opencsv.*;

public class DelayTime {

	//custom a new data type to store the intermediate key
	public static class TextPair implements WritableComparable<TextPair> {

		private Text transferStop;
		private Text flightDate;

		public TextPair() {
			set(new Text(), new Text());
		}

		public TextPair(Text t1, Text t2) {
			transferStop = t1;
			flightDate = t2;
		}
		@Override
		public int hashCode(){
			return transferStop.hashCode() + flightDate.hashCode();
		}

		public void set(Text left, Text right) {
			transferStop = left;
			flightDate = right;
		}

		public Text gettransferStop() {
			return transferStop;
		}

		public Text getflightDate() {
			return flightDate;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			if (transferStop == null)
				transferStop = new Text();

			if (flightDate == null)
				flightDate = new Text();

			transferStop.readFields(in);
			flightDate.readFields(in);

		}

		@Override
		public void write(DataOutput out) throws IOException {
			transferStop.write(out);
			flightDate.write(out);

		}

		@Override
		public boolean equals(Object o) {
			TextPair tp = (TextPair) o;
			return (gettransferStop().equals(tp.gettransferStop()) 
					&& getflightDate().equals(tp.getflightDate()));
		}

		@Override
		public int compareTo(TextPair o) {
			TextPair tp = (TextPair) o;
			int cmp = gettransferStop().compareTo(tp.gettransferStop());
			if (cmp != 0) {
				return cmp;
			}
			return getflightDate().compareTo(tp.getflightDate());
		}
	}

	private static int yearStart = 2007;
	private static int yearEnd = 2008;
	private static int monthStart = 6;
	private static int monthEnd = 5;
	private static String startStop = "ORD";
	private static String endStop = "JFK";

	public static class FlightMapper extends
			Mapper<LongWritable, Text, TextPair, Text> {
		private TextPair outputKey = new TextPair();
		private Text outputValue = new Text();
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			//retrieve the needed value from the current line
			CSVParser parser = new CSVParser();
			String nextLine[] = parser.parseLine(value.toString());
			int year = Integer.parseInt(nextLine[0]);
			int month = Integer.parseInt(nextLine[2]);
			String flightDate;
			String origin = nextLine[11];
			String dest = nextLine[17];
			String depTime;
			String arrTime;
			String arrDelayMinutes;
			double isCancelled = Double.parseDouble(nextLine[41]);
			double isDiverted = Double.parseDouble(nextLine[43]);

			//filter out the records that are not in the required time range and 
			//are cancelled or diverted
			if (((year > yearStart && year < yearEnd)
					|| (year == yearStart && month >= monthStart) 
					|| (year == yearEnd && month <= monthEnd))
					&& (isCancelled == 0.00) && (isDiverted == 0.00)) {
				flightDate = nextLine[5];
				arrDelayMinutes = nextLine[37];
				//emit the needed info of the flight whose origin is "ORD" 
				//but the dest is not "JFK", using its dest and flightdate as key
				if (origin.equals(startStop) && (!dest.equals(endStop))) {
					arrTime = nextLine[35];
					outputKey.set(new Text(dest), new Text(flightDate));
					outputValue.set(origin + ";" + arrTime + ";" + arrDelayMinutes);
					context.write(outputKey, outputValue);
				//emit the needed info of the flight whose origin is not "ORD" 
				//but the dest is "JFK", using its origin and flightdate as key
				} else if ((!origin.equals(startStop)) && dest.equals(endStop)) {
					depTime = nextLine[24];
					outputKey.set(new Text(origin), new Text(flightDate));
					outputValue.set(dest + ";" + depTime + ";" + arrDelayMinutes);
					context.write(outputKey, outputValue);
				}
			}
		}
	}

	public static class DelayReducer extends
			Reducer<TextPair, Text, NullWritable, DoubleWritable> {
		private DoubleWritable result = new DoubleWritable();
		private NullWritable nullKey = NullWritable.get();
		@Override
		public void reduce(TextPair key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			double delaySum = 0.00;
			String data[];
			List<Text> startList = new ArrayList<Text>();
			List<Text> endList = new ArrayList<Text>();
			//split the list of value into two lists: one whose origin is "ORD",
			//the other one whose dest is "JFK" (flight2)
			for (Text val : values) {
				data = val.toString().split(";");
				if (data[0].equals(startStop)) {
					Text startVal = new Text(val.toString());
					startList.add(startVal);
				} else if (data[0].equals(endStop)) {
					Text endVal = new Text(val.toString());
					endList.add(endVal);
				}
			}

			double arrTime;
			double depTime;
			String dataStart[];
			String dataEnd[];
			//find all the two-leg flights, and emit the sum of their delay time
			for (Text start : startList) {
				dataStart = start.toString().split(";");
				arrTime = Double.parseDouble(dataStart[1]);
				for (Text end : endList) {
					dataEnd = end.toString().split(";");
					depTime = Double.parseDouble(dataEnd[1]);
					//check if the departure time of Flight2 is later 
					//than the arrival time Flight1
					if (depTime > arrTime) {
						delaySum += Double.parseDouble(dataStart[2])
								+ Double.parseDouble(dataEnd[2]);
						result.set(delaySum);
						context.write(nullKey, result);
						delaySum = 0.00; //reset the delaySum
					}
				}
			}
		}
	}

	public static int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: delaytime <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "Delay Time");
		job.setJarByClass(DelayTime.class);
		job.setMapperClass(FlightMapper.class);
		job.setReducerClass(DelayReducer.class);

		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(DoubleWritable.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = run(args);
		System.exit(exitCode);
	}
}
