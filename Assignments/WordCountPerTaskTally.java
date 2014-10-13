
import java.io.IOException;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.Iterator;

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

public class WordCountPerTaskTally {

  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, IntWritable>{
    
	//declare a variable instead of a constant to store the number of each word
	private IntWritable num = new IntWritable();
    private Text word = new Text();
    private HashMap<String, Integer> hm;

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      if(hm == null)
    	  hm = new HashMap<String, Integer>();
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
      		if(hm.containsKey(maybeWord)){
      			hm.put(maybeWord, hm.get(maybeWord) + 1);
      		} else {
      			hm.put(maybeWord, 1);
      		}			  
      	}
      }
    }
    
    //output the words and their frequencies
    protected void cleanup(Context context)
    		throws IOException, InterruptedException{
    	if(hm == null)
    		hm = new HashMap<String, Integer>();
    	Iterator<Map.Entry<String, Integer>> it = hm.entrySet().iterator();
        while(it.hasNext()){
     	   Map.Entry<String, Integer> entry = (Map.Entry<String, Integer>)it.next();
     	   word.set(entry.getKey().toString());
     	   num.set((Integer)entry.getValue());
     	   context.write(word, num);
     	   it.remove();
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
		  //otherwise, the corresponding word should be assigned to (ini-77)
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
    job.setJarByClass(WordCountPerTaskTally.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
