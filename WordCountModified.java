import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCountModified {

  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, IntWritable>{
    
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
    	String maybeWord = itr.nextToken();
    	//get the first letter of the current word
    	char firstLetter = maybeWord.charAt(0);
    	//convert the letter to ascii code
    	int ini = (int) firstLetter;	
    	//if the word starts with "m","n","o","p",or "q" (capitalized or not), 
    	//add it to output.
    	if ((77 <= ini && ini <= 81) || (109 <= ini && ini <= 113)){
        word.set(maybeWord);				   
        context.write(word, one);			  
    	}
      }
    }
  }
  
  public static class MyPartitioner extends Partitioner<Text, IntWritable>{
	  @Override
	  //implement the getPartition() method of Partitioner
	  public int getPartition(Text key, IntWritable values,
			  int numPartitions){
		  //get the keyword
		  String keyWord = key.toString();
		  //convert the first letter of the word to ascii code
		  int ini = (int) keyWord.charAt(0);
		  //if the letter is between "m" and "q", then its ascii code (ini) is 
		  //greater than 100, the corresponding word should be assigned to (ini-109);
		  //if the letter is between "M" and "Q", 
		  //the corresponding word should be assigned to (ini-77)
		  return (ini > 100 ? (ini-109) : (ini-77)) % numPartitions;
	  }
  }
  
  public static class IntSumReducer 
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "word count");
    job.setNumReduceTasks(5);
    job.setPartitionerClass(MyPartitioner.class);
    job.setJarByClass(WordCountModified.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
