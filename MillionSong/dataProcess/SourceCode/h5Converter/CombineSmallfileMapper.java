package h5Converter;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CombineSmallfileMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

	//private Text file = new Text();
	private NullWritable nullKey = NullWritable.get();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		//String fileName = context.getConfiguration().get("map.input.file");
		//file.set(fileName);
		context.write(nullKey, value);
	}

}