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


public class Que18 {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		if(args.length != 3){
			System.err.println("TYPE:   hadoop jar <JARNAME> input o1 o2");
			System.exit(2);
		}
		
		Configuration conf1 = new Configuration();
		Job job1 = new Job(conf1,"Finding DataIntensiveComputing class Enroll");
		job1.setJarByClass((Class)Que18.class);
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
		Job job2 = new Job(conf2,"Counting total Enroll");
		job2.setJarByClass((Class)Que18.class);
		job2.setMapperClass((Class)Mapper2.class);
		job2.setReducerClass((Class)Reducer2.class);
		job2.setMapOutputKeyClass((Class)Text.class);
		job2.setMapOutputValueClass((Class)IntWritable.class);
		job2.setOutputKeyClass((Class)Text.class);
		job2.setOutputValueClass((Class)IntWritable.class);
		FileInputFormat.addInputPath((Job)job2, (Path)new Path(args[1].toString()));
		FileOutputFormat.setOutputPath((Job)job2, (Path)new Path(args[2].toString()));
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
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
			
			if((year < 2002) || (year>2016)){
				return;
			}
			
			int enrol = 0;
			try{
				enrol = Integer.parseInt(enrollCurr);
			}
			catch(Exception e){
				return;
			}
			
			if(courseName.equalsIgnoreCase("Data Intensive Computing")){
				String SemYear_CourseName_Id = semName+"_"+courseName+"_"+courseId;
				context.write(new Text(SemYear_CourseName_Id), new IntWritable(enrol));
			}
			else{
				return;
			}
		}
	}
	
	
	public static class Reducer1 extends Reducer<Text,IntWritable,Text,IntWritable>{
		public void reduce(Text key, Iterable<IntWritable> iterVal, Context context) throws IOException, InterruptedException{
			Iterator <IntWritable>iter = iterVal.iterator();
			int max =  0;
			while(iter.hasNext()){
				int enr = iter.next().get();
				if(enr>=max){
					max = enr;
				}
			}
			context.write(key, new IntWritable(max));
		}
	}
	
	public static class Mapper2 extends Mapper<Object, Text, Text, IntWritable>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String [] splitVal = value.toString().split("\\t");
			String []mapKey = splitVal[0].split("_");
			String outKey = mapKey[0]+"_"+mapKey[1];
			context.write(new Text(outKey), new IntWritable(Integer.parseInt(splitVal[1])));
		}
	}
	
	public static class Reducer2 extends Reducer<Text, IntWritable, Text, IntWritable>{
		public void reduce(Text key, Iterable<IntWritable>iterVal, Context context) throws IOException, InterruptedException{
			Iterator <IntWritable> iter = iterVal.iterator();
			int sum = 0;
			while(iter.hasNext()){
				sum = sum + (iter.next().get());
			}
			context.write(key, new IntWritable(sum));
		}
	}
	
	

}
