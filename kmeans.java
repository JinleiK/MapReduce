package bird;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import au.com.bytecode.opencsv.CSVParser;

public class Kmeans {
	//gloable hash map to stroe centers and updated centers
	public static HashMap<String,String> hm1 = new HashMap<String,String>();
	public static HashMap<String,String> hm2 = new HashMap<String,String>();
	public static float dist(String point1,String point2){
		if(point2.equals("")) return (float)100;
		String[] p1 = point1.split("\t");
		String[] p2 = point2.split("\t");
		float lati1=Float.parseFloat(p1[0]);
		float lati2=Float.parseFloat(p2[0]);
		float longti1=Float.parseFloat(p1[1]);
		float longti2=Float.parseFloat(p2[1]);
		float s1 = (lati1-lati2)*(lati1-lati2);
		float s2 = (longti1-longti2)*(longti1-longti2);
		float result = (float) Math.sqrt(s1+s2);
		return result;
	}
	//update the centers
	public static void update(){
    	for(String center_key:hm1.keySet()){
    		hm1.put(center_key, hm2.get(center_key));
    	}
    }
	//check whether converged
	public static boolean converge(){
		for(String center_key:hm1.keySet()){
			String center1 = hm1.get(center_key);
			String center2 = hm2.get(center_key);
			if(center2.equals("")) return false;
			
			if(dist(center1,center2)>1){
				return false;
			}
		}
		return true;
	}
	public static void initial(){
		for(int i=1;i<=12;i++){
			String month = String.valueOf(i)+"_";
			hm1.put(month+"1", "24.5308829\t-82.005558");
			hm1.put(month+"2", "25.5350068\t-80.3431249");
			hm1.put(month+"3", "26.0391\t-80.1956221");
			hm1.put(month+"4", "26.0675773\t-97.8111935");
//			hm1.put(month+"5", "27.4542936\t-82.6127833");
//			hm1.put(month+"6", "29.6928247\t-98.0509186");
//			hm1.put(month+"7", "27.4542936\t-110.6127833");
//			hm1.put(month+"8", "29.6928247\t-105.0509186");
		}
		for(int i=1;i<=12;i++){
			String month = String.valueOf(i)+"_";
			hm2.put(month+"1", "24.5308829\t-82.005558");
			hm2.put(month+"2", "25.5350068\t-80.3431249");
			hm2.put(month+"3", "26.0391\t-80.1956221");
			hm2.put(month+"4", "26.0675773\t-97.8111935");
//			hm2.put(month+"5", "27.4542936\t-82.6127833");
//			hm2.put(month+"6", "29.6928247\t-98.0509186");
//			hm2.put(month+"7", "27.4542936\t-110.6127833");
//			hm2.put(month+"8", "29.6928247\t-105.0509186");
		}
		
	}
	public static class KmeansMapper 
       extends Mapper<Object, Text, Text, Text>{
    
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	String[] s_array = value.toString().split("\t");
    	String latitude = s_array[1];
    	String longtitude = s_array[2];
    	String point = latitude+"\t"+longtitude;
    	int month = Integer.parseInt(s_array[0]);
    		float min = Float.MAX_VALUE;
        	String curCenter="";
        	//compare current point to each center, it will belong to the closest one
        	for(int i=1;i<=4;i++){
        		String center_key = String.valueOf(month)+"_"+i;
        		String center_point = hm1.get(center_key);
        		float distance = dist(point,center_point);
        		if(distance<min){
        			min = distance;
        			curCenter=center_key;
        		}
        	}
        	context.write(new Text(curCenter),new Text(point));
    }
  }
  public static void p(Object o){
		System.out.println(o);
	}
  public static class KmeansReducer 
       extends Reducer<Text,Text,Text,Text> {
    private FloatWritable result = new FloatWritable();
    float max = (float)0;
    public void reduce(Text key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {
    	float total_lati = (float)0;
    	float total_longti = (float)0;
    	float num = (float)0;
    	for (Text point : values){
    		String[] p = point.toString().split("\t");
    		float lati = Float.parseFloat(p[0]);
    		float longti = Float.parseFloat(p[1]);
    		total_lati+=lati;
    		total_longti+=longti;
    		num++;
    	}
    	//get the mean for the new center and update into hm2
    	float avg_lati = total_lati/num;
    	float avg_longti = total_longti/num;
    	String new_center = String.valueOf(avg_lati)+"\t"+avg_longti;
    	hm2.put(key.toString(), new_center);
    }
    
    public void cleanup(Reducer.Context context)throws IOException,
    InterruptedException{
    	for(String center_key:hm1.keySet()){
    		context.write(new Text(center_key),new Text(hm1.get(center_key)));
    	}
    }
  }
  
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }
    Kmeans.initial();
    int i=0;
    do{
    	update();
    	Job job = new Job(conf, "k-means");
    	p("iteration:"+String.valueOf(i+1)+" start.");
    	FileSystem fs = FileSystem.get(conf);
    	Path out = new Path(otherArgs[1]);
    	//if output folder exists, remove it so next iteration can be executed
    	if (fs.exists(out))
    	    fs.delete(out, true);
        job.setJarByClass(Kmeans.class);
        job.setMapperClass(KmeansMapper.class);
        job.setReducerClass(KmeansReducer.class);
        job.setNumReduceTasks(10);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        job.waitForCompletion(true);
        i++;
    }while(!converge());
    p("after "+i+" iteration, it get converged.");
    
  }
}
