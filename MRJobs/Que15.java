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


public class Que15 {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		
		if(args.length != 2){
			System.err.println("TYPE:   hadoop jar <JARNAME> input o1");
			System.exit(2);
		}
		Configuration conf1 = new Configuration();
		Job job1 = new Job(conf1,"per year class enroll");
		job1.setJarByClass((Class)Que15.class);
		job1.setMapperClass((Class)Mapper1.class);
		job1.setReducerClass((Class)Reducer1.class);
		job1.setMapOutputKeyClass((Class)Text.class);
		job1.setMapOutputValueClass((Class)IntWritable.class);
		job1.setOutputKeyClass((Class)Text.class);
		job1.setOutputValueClass((Class)IntWritable.class);
		FileInputFormat.addInputPath((Job)job1, (Path)new Path(args[0].toString()));
		FileOutputFormat.setOutputPath((Job)job1, (Path)new Path(args[1].toString()));
		
		System.exit(job1.waitForCompletion(true) ? 0 : 1);
	}
	
	public static class Mapper1 extends Mapper<Object,Text,Text,IntWritable>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
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
			
			if(weekDays.equalsIgnoreCase("UNKWN") || weekDays.equalsIgnoreCase("TR") || weekDays.equalsIgnoreCase("R")){
				return;
			}
			
			if(timings.equalsIgnoreCase("Unknown")){
				return;
			}
			
			if((year < 2015) || (year>2016)){
				return;
			}
			
			if(courseName.equalsIgnoreCase("General Chemistry")){
				String courseName_timing = courseName+"_"+timings;
				int enrollment = 0;
				try{
					enrollment = Integer.parseInt(enrollCurr);
				}
				catch(Exception e){
					return;
				}
				context.write(new Text(courseName_timing), new IntWritable(enrollment));
			}
			else{
				return;
			}
		}
	}
	
	
	public static class Reducer1 extends Reducer<Text, IntWritable, Text, IntWritable>{
		public void reduce(Text key, Iterable<IntWritable> iterVal, Context context) throws IOException, InterruptedException{
			int totalCap = 0;
			Iterator <IntWritable> iter = iterVal.iterator();
			while(iter.hasNext()){
				totalCap = totalCap + iter.next().get();
			}
			context.write(key, new IntWritable(totalCap));
		}
	}
	

}
