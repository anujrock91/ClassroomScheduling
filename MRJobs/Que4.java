import java.io.IOException;
import java.io.PrintStream;
import java.util.Iterator;
import java.util.StringTokenizer;
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


public class Que4 {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		if(args.length != 5){
			System.err.println("TYPE:   hadoop jar <JARNAME> input o1 o2 o3 o4");
			System.exit(2);
		}
		Configuration conf = new Configuration();
		Job job1 = new Job(conf, "Class Capacity");
		job1.setJarByClass((Class)Que4.class);
		job1.setMapperClass((Class)Mapper1.class);
		job1.setReducerClass((Class)Reducer1.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(IntWritable.class);
		job1.setOutputKeyClass((Class)Text.class);
		job1.setOutputValueClass((Class)IntWritable.class);
		FileInputFormat.addInputPath((Job)job1, (Path)new Path(args[0].toString()));
		FileOutputFormat.setOutputPath((Job)job1, (Path)new Path(args[1].toString()));
		
		job1.waitForCompletion(true);
		
		Configuration conf1 = new Configuration();
		Job job2 = new Job(conf1,"Capcaity more than 100");
		job2.setJarByClass((Class)Que4.class);
		job2.setMapperClass((Class)Mapper2.class);
		job2.setReducerClass((Class)Reducer2.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(IntWritable.class);
		job2.setOutputKeyClass((Class)Text.class);
		job2.setOutputValueClass((Class)IntWritable.class);
		FileInputFormat.addInputPath((Job)job2, (Path)new Path(args[1].toString()));
		FileOutputFormat.setOutputPath((Job)job2, (Path)new Path(args[2].toString()));
		
		job2.waitForCompletion(true);
		
		Configuration conf2 = new Configuration();
		Job job3 =  new Job(conf2,"Free on M W");
		job3.setJarByClass((Class)Que4.class);
		job3.setMapperClass((Class)Mapper3.class);
		job3.setReducerClass((Class)Reducer3.class);
		job3.setMapOutputKeyClass((Class)Text.class);
		job3.setMapOutputValueClass((Class)IntWritable.class);
		job3.setOutputKeyClass((Class)Text.class);
		job3.setOutputValueClass((Class)IntWritable.class);
		FileInputFormat.addInputPath((Job)job3, (Path)new Path(args[2].toString()));
		FileOutputFormat.setOutputPath((Job)job3, (Path)new Path(args[3].toString()));
		
		job3.waitForCompletion(true);
		
		Configuration conf3 = new Configuration();
		Job job4 =  new Job(conf3,"Free on M W F");
		job4.setJarByClass((Class)Que4.class);
		job4.setMapperClass((Class)Mapper4.class);
		job4.setReducerClass((Class)Reducer4.class);
		job4.setMapOutputKeyClass((Class)Text.class);
		job4.setMapOutputValueClass((Class)Text.class);
		job4.setOutputKeyClass((Class)Text.class);
		job4.setOutputValueClass((Class)IntWritable.class);
		FileInputFormat.addInputPath((Job)job4, (Path)new Path(args[3].toString()));
		FileOutputFormat.setOutputPath((Job)job4, (Path)new Path(args[4].toString()));
		
		System.exit(job4.waitForCompletion(true) ? 0 : 1);
		
	}
	
	public static class Mapper1 extends Mapper<Object, Text, Text, IntWritable>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String originalText = value.toString();
			String []splittedText = originalText.split(",");
			splittedText[1] = splittedText[1].trim();
			splittedText[2] = splittedText[2].trim();
			String []splittedText2 = splittedText[2].split(" ");
			splittedText[3] = splittedText[3].trim();
			splittedText[4] = splittedText[4].trim();
			splittedText[5] = splittedText[5].trim();
			splittedText[6] = splittedText[6].trim();
			
			splittedText2[0] = splittedText2[0].trim();
			if(splittedText2[0].equalsIgnoreCase("Unknown") || splittedText2[0].equalsIgnoreCase("Arr")){
				return;
			}
			if(splittedText[1].equalsIgnoreCase("Spring 2016")){
					String className_time_Day = splittedText[2]+"_"+splittedText[4]+"_"+splittedText[3];
					try{
						context.write(new Text(className_time_Day), new IntWritable(Integer.parseInt(splittedText[8].toString())));
					}
					catch(Exception e){
						return;
					}
			}
			else{
				return;
			}
		}
	}
	
	public static class Reducer1 extends Reducer<Text,IntWritable,Text,IntWritable>{
		public void reduce(Text key, Iterable<IntWritable> iterVal, Context context) throws IOException, InterruptedException{
			Iterator<IntWritable> i = iterVal.iterator();
			int capacity = 0;
			int sum = 0;
			while(i.hasNext()){
				capacity = capacity+(i.next().get());
				++sum;
			}
			int finalCapacity = (capacity/sum);
			context.write(key,new IntWritable(finalCapacity));
		}
	}
	
	public static class Mapper2 extends Mapper<Object, Text, Text, IntWritable>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String []splitText = value.toString().split("\\t");
			int capacity = Integer.parseInt(splitText[1].toString());
			if(capacity < 60){
				return;
			}
			else{
				context.write(new Text(splitText[0]), new IntWritable(capacity));
			}
		}
	}
	
	public static class Reducer2 extends Reducer<Text, IntWritable, Text, IntWritable>{
		public void reduce(Text key, Iterable<IntWritable> iterVal, Context context) throws IOException, InterruptedException{
			Iterator <IntWritable> iter = iterVal.iterator();
			int sum = 0;
			int capacitySum = 0;
			while(iter.hasNext()){
				capacitySum = capacitySum+(iter.next().get());
				++sum;
			}
			int capacity =  (capacitySum/sum);
			context.write(key,new IntWritable(capacity));
		}
	}
	
	public static class Mapper3 extends Mapper<Object,Text,Text,IntWritable>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String [] splitString = value.toString().split("\\t");
			String [] className_time_Day = splitString[0].split("_");
			String className = className_time_Day[0];
			String timings = className_time_Day[1];
			String day = className_time_Day[2];
			
			if(timings.equals("5:00PM - 5:59PM") && day.equalsIgnoreCase("M")){
				String className1 = className+"_"+"WF";
				context.write(new Text(className1), new IntWritable(Integer.parseInt(splitString[1].toString())));
			}
			if(timings.equals("5:00PM - 5:59PM") && day.equalsIgnoreCase("W")){
				String className2 = className+"_"+"MF";
				context.write(new Text(className2), new IntWritable(Integer.parseInt(splitString[1].toString())));
			}
			if(timings.equals("5:00PM - 5:59PM") && day.equalsIgnoreCase("F")){
				String className3 = className+"_"+"MW";
				context.write(new Text(className3), new IntWritable(Integer.parseInt(splitString[1].toString())));
			}
			if(timings.equals("5:00PM - 5:59PM") && day.equalsIgnoreCase("MWF")){
				return;
			}
			if(day.equalsIgnoreCase("TR")){
				return;
			}
			String className4 = className+"_"+"MWF";
			context.write(new Text(className4), new IntWritable(Integer.parseInt(splitString[1].toString())));
			
		}
	}
	
	public static class Reducer3 extends Reducer<Text,IntWritable, Text, IntWritable>{
		public void reduce(Text key,Iterable<IntWritable> iterVal, Context context) throws IOException, InterruptedException{
			Iterator <IntWritable> iter = iterVal.iterator();
			int sum = 0;
			int CapacitySum = 0;
			while(iter.hasNext()){
				CapacitySum = (CapacitySum + (iter.next().get()));
				++sum;
			}
			int capacity = (CapacitySum/sum);
			context.write(key, new IntWritable(capacity));
		}
	}
	
	public static class Mapper4 extends Mapper<Object,Text,Text,Text>{
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
			String[] splitString = value.toString().split("\\t");
			String []splitString0 = splitString[0].split("_");
			String className = splitString0[0];
			String scheduleFree = splitString0[1];
			String scheduleFreeCap = scheduleFree+"_"+splitString[1];
			context.write(new Text(className), new Text(scheduleFreeCap));
		}
	}
	
	public static class Reducer4 extends Reducer<Text,Text,Text,IntWritable>{
		public void reduce(Text key, Iterable<Text> iterVal,Context context) throws IOException, InterruptedException{
			char M = 'M';
			char W = 'W';
			char F = 'F';
			int capacityFinal = 0;
			Iterator <Text> iter = iterVal.iterator();
			while(iter.hasNext()){
				String s = iter.next().toString();
				String [] sSplit = s.split("_");
				String sch = sSplit[0];
				String cap = sSplit[1];
				int Mflag = 0;
				for(int i=0;i<sch.length();i++){
					if(M == sch.charAt(i)){
						Mflag = 1;
						break;
					}
				}
				if(Mflag != 1){
					M = 'X';
				}
				int Wflag = 0;
				for(int i=0;i<sch.length();i++){
					if(W == sch.charAt(i)){
						Wflag = 1;
						break;
					}
				}
				if(Wflag != 1){
					W = 'X';
				}
				int Fflag = 0;
				for(int i=0;i<sch.length();i++){
					if(F == sch.charAt(i)){
						Fflag = 1;
						break;
					}
				}
				if(Fflag != 1){
					F = 'X';
				}
				if(capacityFinal < Integer.parseInt(cap)){
					capacityFinal = Integer.parseInt(cap);
				}
			}
			
			if(M == 'X' && W == 'X' && F == 'X'){
				return;
			}
			else{
				String FinalKey = key.toString()+"_"+M+W+F;
				context.write(new Text(FinalKey), new IntWritable(capacityFinal));
			}
			
		}
	}

}
