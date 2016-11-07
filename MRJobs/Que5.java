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


public class Que5 {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		
		if(args.length != 3){
			System.err.println("TYPE:   hadoop jar <JARNAME> input o1 o2");
			System.exit(2);
		}
		Configuration conf1 = new Configuration();
		Job job1 = new Job(conf1,"Summer 2016 cap > 200 Free");
		job1.setJarByClass((Class)Que5.class);
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
		Job job2 = new Job(conf2,"Summer 2016 Free on M at 6-6:59PM cap >200");
		job2.setJarByClass((Class)Que5.class);
		job2.setMapperClass((Class)Mapper2.class);
		job2.setReducerClass((Class)Reducer2.class);
		job2.setMapOutputKeyClass((Class)Text.class);
		job2.setMapOutputValueClass((Class)Text.class);
		job2.setOutputKeyClass((Class)Text.class);
		job2.setOutputValueClass((Class)IntWritable.class);
		FileInputFormat.addInputPath((Job)job2, (Path)new Path(args[1].toString()));
		FileOutputFormat.setOutputPath((Job)job2, (Path)new Path(args[2].toString()));
		
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
	
	public static class Mapper1 extends Mapper<Object, Text, Text, IntWritable>{
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
			
			
			if(classLocSplit[0].equalsIgnoreCase("Unknown") || classLocSplit[0].equalsIgnoreCase("Arr")){
				return;
			}
			
			if(weekDays.equalsIgnoreCase("UNKWN")){
				return;
			}
			
			if(timings.equalsIgnoreCase("Unknown")){
				return;
			}
			
			if(!(semName.equalsIgnoreCase("Summer 2016"))){
				return;
			}
			
			int capacity = 0;
			try{
				capacity = Integer.parseInt(cap.toString());
			}
			catch(Exception e){
				return;
			}
			
			if(capacity < 200){
				return;
			}
			
			String className_Day_time = classLoc+"_"+weekDays+"_"+timings;
			context.write(new Text(className_Day_time), new IntWritable(capacity));			
		}
	}
	
	
	public static class Reducer1 extends Reducer<Text, IntWritable, Text, IntWritable>{
		public void reduce(Text key, Iterable<IntWritable> iterVal, Context context) throws IOException, InterruptedException{
			Iterator <IntWritable>iter = iterVal.iterator();
			int maxCap = 0;
			int a = 0;
			while(iter.hasNext()){
				a=iter.next().get();
				if(a>maxCap){
					maxCap = a;
				}
			}	
			context.write(key,new IntWritable(maxCap));
		}
	}
	
	
	public static class Mapper2 extends Mapper<Object, Text, Text, Text>{
		public void map(Object key,Text value, Context context) throws IOException, InterruptedException{
			String [] splitVal = value.toString().split("\\t");
			String []inpKey = splitVal[0].split("_");
			String sch = inpKey[1];
			String timings = inpKey[2];
			
			for(int i=0;i<sch.length();i++){
				if(sch.charAt(i) == 'M'){
					
					if(timings.equals("6:00PM - 6:59PM")){
						String outKey = inpKey[0];
						String outVal = splitVal[1]+'_'+'N';
						context.write(new Text(outKey), new Text(outVal));
						return;
					}
					else{
						String outKey = inpKey[0];
						String outVal = splitVal[1]+'_'+'Y';
						context.write(new Text(outKey), new Text(outVal));
						return;
					}
				}
			}
			String outKey = inpKey[0];
			String outVal = splitVal[1]+'_'+'Y';
			context.write(new Text(outKey), new Text(outVal));
			
		}
	}
	
	
	public static class Reducer2 extends Reducer<Text,Text,Text,IntWritable>{
		public void reduce(Text key, Iterable<Text> iterVal, Context context) throws IOException, InterruptedException{
			Iterator <Text> iter = iterVal.iterator();
			int cap = 0;
			while(iter.hasNext()){
				String s = iter.next().toString();
				String [] sSplit = s.split("_");
				if(sSplit[1].equalsIgnoreCase("N")){
					return;
				}
				if(Integer.parseInt(sSplit[0].toString()) > cap){
					cap = Integer.parseInt(sSplit[0].toString());
				}
			}
			context.write(key, new IntWritable(cap));
		}
	}
	
	

}
