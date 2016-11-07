import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Que20 {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		
		if(args.length !=4){
			System.err.println("TYPE:   hadoop jar <JARNAME> input o1 o2 o3");
			System.exit(2);
		}
		
		Configuration conf1 = new Configuration();
		Job job1 = new Job(conf1,"Finding various subjects listed in a time slot");
		job1.setJarByClass((Class)Que20.class);
		job1.setMapperClass((Class)Mapper1.class);
		job1.setReducerClass((Class)Reducer1.class);
		job1.setMapOutputKeyClass((Class)Text.class);
		job1.setMapOutputValueClass((Class)IntWritable.class);
		job1.setOutputKeyClass((Class)Text.class);
		job1.setOutputValueClass((Class)IntWritable.class);
		FileInputFormat.addInputPath((Job)job1, (Path)new Path(args[0].toString()));
		FileOutputFormat.setOutputPath((Job)job1, (Path)new Path(args[1].toString()));
		
		job1.waitForCompletion(true);
		
		Configuration conf2 = new Configuration();
		Job job2 = new Job(conf2,"Finding All the subjects in a time slot in last 3 years");
		job2.setJarByClass((Class)Que20.class);
		job2.setMapperClass((Class)Mapper2.class);
		job2.setReducerClass((Class)Reducer2.class);
		job2.setMapOutputKeyClass((Class)Text.class);
		job2.setMapOutputValueClass((Class)IntWritable.class);
		job2.setOutputKeyClass((Class)Text.class);
		job2.setOutputValueClass((Class)IntWritable.class);
		FileInputFormat.addInputPath((Job)job2, (Path)new Path(args[1].toString()));
		FileOutputFormat.setOutputPath((Job)job2, (Path)new Path(args[2].toString()));
		
		job2.waitForCompletion(true);
		
		
		Configuration conf3 = new Configuration();
		Job job3 = new Job(conf3,"Finding All the subjects in a time slot in last 3 years");
		job3.setJarByClass((Class)Que20.class);
		job3.setMapperClass((Class)Mapper3.class);
		job3.setReducerClass((Class)Reducer3.class);
		job3.setMapOutputKeyClass((Class)Text.class);
		job3.setMapOutputValueClass((Class)IntWritable.class);
		job3.setOutputKeyClass((Class)Text.class);
		job3.setOutputValueClass((Class)IntWritable.class);
		FileInputFormat.addInputPath((Job)job3, (Path)new Path(args[2].toString()));
		FileOutputFormat.setOutputPath((Job)job3, (Path)new Path(args[3].toString()));
		
		System.exit(job3.waitForCompletion(true) ? 0 : 1);

	}
	
	public static class Mapper1 extends Mapper<Object, Text, Text, IntWritable>{
		public void map(Object key,Text value, Context context) throws IOException, InterruptedException{
			String []splitVal = value.toString().split(",");
			String semName = splitVal[1].trim();
			String classLoc = splitVal[2].trim();
			String weekDays = splitVal[3].trim();
			String timings = splitVal[4].trim();
			String courseId = splitVal[5].trim();
			String courseName = splitVal[6].trim();
			String enrollCurr = splitVal[7].trim();
			String cap = splitVal[8].trim();
			
			String []classLocSplit = classLoc.split(" ");
			classLocSplit[0] = classLocSplit[0].trim();
			
			String [] semNameSplit = semName.split(" ");
			int year = 0;			
			try{
				 year = Integer.parseInt(semNameSplit[1]);
			}
			catch(Exception e){
				return;
			}
			
			if(classLocSplit[0].equalsIgnoreCase("Unknown") || classLocSplit[0].equalsIgnoreCase("Arr")){
				return;
			}
			
			if(weekDays.equalsIgnoreCase("UNKWN")){
				return;
			}
			
			if(timings.equalsIgnoreCase("Unknown")){
				return;
			}
			
			if(!(semName.equalsIgnoreCase("Spring 2016"))){
				return;
			}
			
			String SemName_SubName_Time_Day = semName+"_"+courseName+"_"+timings+"_"+weekDays;
			context.write(new Text(SemName_SubName_Time_Day), new IntWritable(1));		
		}
	}
	
	
	public static class Reducer1 extends Reducer<Text, IntWritable,Text, IntWritable>{
		public void reduce(Text key, Iterable<IntWritable> iterVal, Context context) throws IOException, InterruptedException{
			context.write(key, new IntWritable(1));
		}
	}
	
	
	public static class Mapper2 extends Mapper<Object, Text, Text, IntWritable>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String splitVal[] = value.toString().split("\\t");
			String splitVal0Split []= splitVal[0].split("_");
			String time_Day = splitVal0Split[2]+"_"+splitVal0Split[3];
			context.write(new Text(time_Day), new IntWritable(1));
		}
	}
	
	public static class Reducer2 extends Reducer<Text,IntWritable,Text,IntWritable>{
		public void reduce(Text key, Iterable<IntWritable> iterVal,Context context) throws IOException, InterruptedException{
			Iterator <IntWritable> iter = iterVal.iterator();
			int sum = 0;
			while(iter.hasNext()){
				sum = sum + (iter.next().get());
			}
			context.write(key, new IntWritable(sum));
		}
	}
	
	public static class Mapper3 extends Mapper<Object, Text, Text, IntWritable>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String splitVal[] = value.toString().split("\\t");
			String splitVal0Split []= splitVal[0].split("_");
			if(splitVal0Split[1].equalsIgnoreCase("ARR")){
				return;
			}
			if(splitVal0Split[1].equalsIgnoreCase("M-F")){
				String outKey = splitVal0Split[0]+"_"+"M";
				context.write(new Text(outKey), new IntWritable(Integer.parseInt(splitVal[1])));
				outKey = splitVal0Split[0]+"_"+"T";
				context.write(new Text(outKey), new IntWritable(Integer.parseInt(splitVal[1])));
				outKey = splitVal0Split[0]+"_"+"W";
				context.write(new Text(outKey), new IntWritable(Integer.parseInt(splitVal[1])));
				outKey = splitVal0Split[0]+"_"+"R";
				context.write(new Text(outKey), new IntWritable(Integer.parseInt(splitVal[1])));
				outKey = splitVal0Split[0]+"_"+"F";
				context.write(new Text(outKey), new IntWritable(Integer.parseInt(splitVal[1])));
				return;
			}
			
			for(int i=0;i<splitVal0Split[1].length();i++){
				String outKey = splitVal0Split[0]+"_"+splitVal0Split[1].charAt(i);
				context.write(new Text(outKey), new IntWritable(Integer.parseInt(splitVal[1])));
			}
		}
	}
	
	public static class Reducer3 extends Reducer<Text, IntWritable,Text, IntWritable>{
		public void reduce(Text key, Iterable<IntWritable> iterVal, Context context) throws IOException, InterruptedException{
			int sum = 0;
			Iterator<IntWritable>iter = iterVal.iterator();
			while(iter.hasNext()){
				sum = sum +(iter.next().get());
			}
			context.write(key, new IntWritable(sum));;
		}
	}

}
