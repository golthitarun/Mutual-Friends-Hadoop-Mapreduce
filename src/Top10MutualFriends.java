package JavaHDFS.JavaHDFS;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import JavaHDFS.JavaHDFS.MatrixMultiply.Map1;
import JavaHDFS.JavaHDFS.MatrixMultiply.Map2;
import JavaHDFS.JavaHDFS.MatrixMultiply.Reduce1;
import JavaHDFS.JavaHDFS.MatrixMultiply.Reduce2;

public class Top10MutualFriends extends Configured implements Tool{
	public static class Map1 extends Mapper<LongWritable, Text, Text, Text>{
		
		private Text pair = new Text(); // type of output key
		private Text List = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] line = value.toString().split("\\t");
			String User = line[0];
			if (line.length ==2) {
				ArrayList<String> FriendsList = new ArrayList<String>(Arrays.asList(line[1].split("\\,")));
				for(String Friend:FriendsList){				
					String FriendPair = (Integer.parseInt(User) < Integer.parseInt(Friend))?User+"\t"+Friend:Friend+"\t"+User;
					ArrayList<String> temp = new ArrayList<String>(FriendsList);
					temp.remove(Friend); 
					String listString = String.join(",", temp);
					pair.set(FriendPair);
					List.set(listString);
					context.write(pair,List);
				}
			}
			
		}
	}
		
	public static class Reduce1 extends Reducer<Text,Text,Text,Text> {
		private Text result = new Text();
		public String Mutual(String s1,String s2,int i) {
			HashSet<String> map = new HashSet<String>();
			String[] s1_split = s1.split("\\,");
			String[] s2_split = s2.split("\\,");
			String result = "";
			for(String s:s1_split) {
				map.add(s);
			}
			for(String s:s2_split) {
				if (map.contains(s)){
					result +=s+",";
				}
			}
			return result;	
		}
		
		
		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			String[] Friend_key_values = new String[2];
			int i=0;
			for(Text value:values){
				Friend_key_values[i++] = value.toString();
			}
			
			result.set(Mutual(Friend_key_values[0],Friend_key_values[1],i));
			i++;
			context.write(key,result);// create a pair <keyword, number of occurences>
		}
	}
	
	public static class Map2 extends Mapper<LongWritable, Text, IntWritable, Text>{
		
		IntWritable k = new IntWritable(); 
		Text v = new Text();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] line = value.toString().split("\\t");
			String Friend_pair = line[0]+"\t"+line[1];
		
			int count =0;
			if (line.length == 3) {
				ArrayList<String> FriendsList = new ArrayList<String>(Arrays.asList(line[2].split("\\,")));
				count = FriendsList.size();	
				v.set(Friend_pair);
				}			
			else {
				v.set(Friend_pair);
			}
			k.set(count);
			context.write(k,v);
			
		}
	}
	
	public static class Reduce2 extends Reducer<IntWritable,Text,IntWritable,Text>{
		Text v = new Text();
		IntWritable k = new IntWritable();
		private int print_count = 0;
		public void reduce(IntWritable key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			for(Text value:values) {
				String out = value.toString();
				if (print_count == 10) {
					break;
				}
				else {
					String[] splitter = out.split("\\t");
					v.set(splitter[0]+"\t"+Integer.toString(key.get()));
					k.set(Integer.parseInt(splitter[1]));
					context.write(k,v);
					print_count ++;
				}
								
			}
		}			
	}
	public static class customComparator extends WritableComparator{
		public customComparator(){
			super(IntWritable.class, true);
		}
		@Override
		public int compare(WritableComparable w1,WritableComparable w2) {
			IntWritable key1 = (IntWritable) w1;
			IntWritable key2 = (IntWritable) w2;
			return -1 * key1.compareTo(key2);
		}
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Top10MutualFriends(), args);
		System.exit(exitCode);
	}
	
	
	public int run(String[] args) throws Exception {
		
	  	Configuration conf1 = new Configuration();
	  	String[] otherArgs = new GenericOptionsParser(conf1, args).getRemainingArgs();
	  	// get all args
	  	if (otherArgs.length != 2) {
	  	System.out.println(otherArgs[0]);
	  	System.err.println("Usage: Top10MutualFriends <in> <out>");
	  	System.exit(2);
	  	}
	  	// create a job with name "Mutual Friends"
	  	Job job1 = new Job(conf1, "Top10MutualFriends");
	  	job1.setJarByClass(Top10MutualFriends.class);
	  	job1.setMapperClass(Map1.class);
	  	job1.setReducerClass(Reduce1.class);
	  	// set output key type
	  	job1.setOutputKeyClass(Text.class);
	  	// set output value type
	  	job1.setOutputValueClass(Text.class);
	  	//set the HDFS path of the input data
	  	FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
	  	// set the HDFS path for the output
	  	FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]+ "/temp"));
	  	//Wait till job completion
	  	job1.waitForCompletion(true);
	    Configuration conf2 = new Configuration();
	    
	    Job job2 = new Job(conf2,"Top10MutualFriends");
	    job2.setJarByClass(MatrixMultiply.class);
	    job2.setJobName("Top 10 Mutual Friends Second Job");

	    FileInputFormat.setInputPaths(job2, new Path(otherArgs[1] + "/temp"));
	    FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]+"/final"));

	    job2.setSortComparatorClass(customComparator.class);
	    job2.setMapperClass(Map2.class);
	    job2.setReducerClass(Reduce2.class);
	    

	    job2.setOutputKeyClass(IntWritable.class);
	    job2.setOutputValueClass(Text.class);
	    	  
	  	FileSystem hdfs = FileSystem.get(conf1);
	  	
	  	// delete temp directory
	  	Path temp = new Path(otherArgs[1] + "/temp");
	  	if (job2.waitForCompletion(true)) {
	  		if (hdfs.exists(temp)) {
		  	    hdfs.delete(temp, true);
		  	}
	  		return 1;
	  	}
	  	else {
	  		return 0;
	  	}
	  	
}	

}

