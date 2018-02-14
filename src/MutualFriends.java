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


public class MutualFriends {
	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		
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
		
	public static class Reduce extends Reducer<Text,Text,Text,Text> {
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
				System.out.print(s+"% ");
				if (map.contains(s)){
					result +=s+",";
					System.out.print(s+"& ");
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
			
			System.out.println(key+" -- "+Friend_key_values[0] + " ~~~ "+Friend_key_values[1]);
			result.set(Mutual(Friend_key_values[0],Friend_key_values[1],i));
			i++;
			context.write(key,result);// create a pair <keyword, number of occurences>
		}
	}
		
	public static void main(String[] args) throws Exception {
		  	Configuration conf = new Configuration();
		  	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		  	// get all args
		  	if (otherArgs.length != 2) {
		  	System.out.println(otherArgs[0]);
		  	System.err.println("Usage: MutualFriends <in> <out>");
		  	System.exit(2);
		  	}
		  	// create a job with name "wordcount"
		  	Job job = new Job(conf, "MutualFriends");
		  	job.setJarByClass(MutualFriends.class);
		  	job.setMapperClass(Map.class);
		  	job.setReducerClass(Reduce.class);
		  	// uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);
		  	// set output key type
		  	job.setOutputKeyClass(Text.class);
		  	// set output value type
		  	job.setOutputValueClass(Text.class);
		  	//set the HDFS path of the input data
		  	FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		  	// set the HDFS path for the output
		  	FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		  	//Wait till job completion
		  	System.exit(job.waitForCompletion(true) ? 0 : 1);
	}	

}
