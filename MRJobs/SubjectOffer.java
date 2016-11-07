
import java.io.IOException;
import java.io.PrintStream;
import java.util.Iterator;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class SubjectOffer {

	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		
		if(args.length != 3){
			System.err.println("TYPE:   hadoop jar <JARNAME> input o1 o2");
			System.exit(2);
		}
		
		Configuration conf = new Configuration();
		
		Job job = new Job(conf, "Course");
		job.setJarByClass((Class)SubjectOffer.class);
		job.setMapperClass((Class)Mapper1.class);
		job.setReducerClass((Class)Reducer1.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath((Job)job, (Path)new Path(args[0].toString()));
		FileOutputFormat.setOutputPath((Job)job, (Path)new Path(args[1].toString()));
		job.waitForCompletion(true);
		
		Configuration conf2 = new Configuration();
		
		Job job2 = new Job(conf2,"Total Subjects");
		job2.setJarByClass((Class)SubjectOffer.class);
		job2.setMapperClass((Class)Mapper2.class);
		job2.setReducerClass((Class)Reducer2.class);
		job2.setMapOutputKeyClass((Class)Text.class);
		job2.setMapOutputValueClass(IntWritable.class);
		job2.setOutputKeyClass((Class)Text.class);
		job2.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath((Job)job2, (Path)new Path(args[1].toString()));
		FileOutputFormat.setOutputPath((Job)job2, (Path)new Path(args[2].toString()));
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
		
	}
	
	public static class Mapper1 extends Mapper<Object, Text, Text, IntWritable>{	
		public void map(Object key, Text value, Context context) throws InterruptedException, IOException{
			String []val = value.toString().split(",");
			val[2] = val[2].trim();
			String [] tempVal2 = val[2].split(" ");
			val[3] = val[3].trim();
			val[4] = val[4].trim();
			if(val[2].equalsIgnoreCase("Unknown") || val[3].equalsIgnoreCase("UNKWN") || val[4].equalsIgnoreCase("Unknown")){
				return;
			}
			if(tempVal2.length > 1){
				tempVal2[0] = tempVal2[0].trim();
				if(tempVal2[0].equalsIgnoreCase("Arr")){
					return;
				}
			}
			String outkey = val[1]+"_"+val[6];
			context.write(new Text(outkey), new IntWritable(1));
		}
	}
	
	public static class Reducer1 extends Reducer<Text,IntWritable,Text,IntWritable>{
		public void reduce(Text key, Iterable<IntWritable> values,Context context)throws IOException, InterruptedException {
			context.write(key, new IntWritable(1));
		}
	}
	
	public static class Mapper2 extends Mapper<Object,Text, Text, IntWritable>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String []val = value.toString().split("\\t");
			String []val0 = val[0].split("_");
			String semYear = val0[0];
			String [] splitSemYear = semYear.split(" ");
			String year = splitSemYear[1];
			
			context.write(new Text(year), new IntWritable(1));
		}
		
	}
	
	public static class Reducer2 extends Reducer<Text,IntWritable,Text,IntWritable>{
		public void reduce(Text key,Iterable<IntWritable> iterVal, Context context) throws IOException, InterruptedException{
			int sum = 0;
			int i = 0;
			Iterator<IntWritable> iter = iterVal.iterator();
			while(iter.hasNext()){
				i = iter.next().get();
				sum = sum+i;
			}
			context.write(key, new IntWritable(sum));
		}
		
	}

}
