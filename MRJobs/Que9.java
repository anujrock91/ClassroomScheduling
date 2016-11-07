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

public class Que9 {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		
		if(args.length != 4){
			System.err.println("TYPE:   hadoop jar <JARNAME> input o1 o2 o3");
			System.exit(2);
		}
		
		
		Configuration conf1 = new Configuration();
		Job job1 = new Job(conf1,"Per Sem Hall Capacity");
		job1.setJarByClass((Class)Que9.class);
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
		Job job2 = new Job(conf2,"Per Sem Total Hall Cap");
		job2.setJarByClass((Class)Que9.class);
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
		Job job3 = new Job(conf3,"Per year Total Hall Cap");
		job3.setJarByClass((Class)Que9.class);
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
			
			if(classLocSplit[0].equalsIgnoreCase("Baldy") || classLocSplit[0].equalsIgnoreCase("Knox") || classLocSplit[0].equalsIgnoreCase("Jacobs") || classLocSplit[0].equalsIgnoreCase("Obrian")){
				
			}
			else{
				return;
			}
			if(weekDays.equalsIgnoreCase("UNKWN")){
				return;
			}
			
			if(timings.equalsIgnoreCase("Unknown")){
				return;
			}
			
			if((year < 2013) || (year>2016)){
				return;
			}
			
			int capacity = 0;
			try{
				capacity = Integer.parseInt(cap);
			}
			catch(Exception e){
				return;
			}
			
			String year_hallName = semName+"_"+classLoc;
			context.write(new Text(year_hallName), new IntWritable(capacity));
		}
	}
	
	
	public static class Reducer1 extends Reducer<Text,IntWritable,Text, IntWritable>{
		public void reduce(Text key,Iterable<IntWritable>iterVal, Context context) throws IOException, InterruptedException{
			Iterator<IntWritable> iter = iterVal.iterator();
			int capacity = 0;
			while(iter.hasNext()){
				int a = iter.next().get();
				if(a > capacity){
					capacity = a;
				}
			}			
			context.write(key, new IntWritable(capacity));
		}
	}
	
	
	public static class Mapper2 extends Mapper<Object,Text,Text,IntWritable>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String [] splitVal = value.toString().split("\\t");
			String []splitVal0 = splitVal[0].split("_");
			String year = splitVal0[0];
			String HallName = splitVal0[1];
			String []HallNameSplit = HallName.split(" ");
			String hall = HallNameSplit[0];
			
			String outKey = year+"_"+hall;
			context.write(new Text(outKey), new IntWritable(Integer.parseInt(splitVal[1].toString())));
		}
	}
	
	
	public static class Reducer2 extends Reducer<Text,IntWritable,Text,IntWritable>{
		public void reduce(Text key, Iterable<IntWritable>iterVal,Context context) throws IOException, InterruptedException{
			int sum = 0;
			Iterator<IntWritable> iter = iterVal.iterator();
			while(iter.hasNext()){
				sum = sum + (iter.next().get());
			}
			
			context.write(key, new IntWritable(sum));
		}
	}
	
	public static class Mapper3 extends Mapper<Object, Text,Text,IntWritable>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String valueSplit[]= value.toString().split("\\t");
			String [] semName_Hall = valueSplit[0].split("_");
			String [] semNameSplit = semName_Hall[0].split(" ");
			String year = semNameSplit[1];
			String outKey = year+"_"+semName_Hall[1];
			
			context.write(new Text(outKey), new IntWritable(Integer.parseInt(valueSplit[1].toString())));
		}
	}
	
	public static class Reducer3 extends Reducer<Text,IntWritable,Text,IntWritable>{
		public void reduce(Text key, Iterable<IntWritable> iterVal, Context context) throws IOException, InterruptedException{
			int sum = 0;
			Iterator <IntWritable> iter = iterVal.iterator();
			while(iter.hasNext()){
				sum = sum + (iter.next().get());
			}
			context.write(key, new IntWritable(sum));
		}
	}
	
}
