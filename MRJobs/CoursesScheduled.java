
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CoursesScheduled {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if (args.length != 3) {
			System.err.println("TYPE:   hadoop jar <JARNAME> input o1 o2");
			System.exit(2);
		}
		Configuration conf = new Configuration();

		Job job = new Job(conf, "courses scheduled in each semester");
		job.setJarByClass((Class) CoursesScheduled.class);
		job.setMapperClass((Class) Mapper1.class);
		job.setReducerClass((Class) Reducer1.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath((Job) job, (Path) new Path(args[0].toString()));
		FileOutputFormat.setOutputPath((Job) job, (Path) new Path(args[1].toString()));
		job.waitForCompletion(true);

		Configuration conf2 = new Configuration();
		Job job2 = new Job(conf2, "courses scheduled in last few years in various halls");
		job2.setJarByClass((Class) CoursesScheduled.class);
		job2.setMapperClass((Class) Mapper2.class);
		job2.setReducerClass((Class) Reducer2.class);
		job2.setMapOutputKeyClass((Class) Text.class);
		job2.setMapOutputValueClass(IntWritable.class);
		job2.setOutputKeyClass((Class) Text.class);
		job2.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath((Job) job2, (Path) new Path(args[1].toString()));
		FileOutputFormat.setOutputPath((Job) job2, (Path) new Path(args[2].toString()));
		System.exit(job2.waitForCompletion(true) ? 0 : 1);

	}

	public static class Mapper1 extends Mapper<Object, Text, Text, IntWritable> {
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws InterruptedException, IOException {
			String[] terms = value.toString().split(",");
			terms[1] = terms[1].trim();
			terms[2] = terms[2].trim();
			terms[3] = terms[3].trim();
			terms[6] = terms[6].trim();
			terms[5] = terms[5].trim();
			terms[7] = terms[7].trim();
			terms[8] = terms[8].trim();
			StringBuilder semesterHallCourseKey = new StringBuilder();
			String[] roomName = terms[2].split(" ");
			String[] semester = terms[1].split(" ");
			String year = semester[1];
			String season = semester[0];
			String hallName = roomName[0];
			String courseId = terms[5];
			String courseName = terms[6];
			String enrollment = terms[7];
			String capacity = terms[8];
			try {

				// data cleaning code
				if (roomName.length == 0) {
					return;
				} else {
					hallName = hallName.trim();
					if (hallName.equalsIgnoreCase("Unknown") || hallName.equalsIgnoreCase("Arr")) {
						return;
					}

				}

				Integer yearVal = Integer.parseInt(semester[1]);
				if (yearVal < 2014 || yearVal > 2016) {
					return;
				}

				semesterHallCourseKey.append(season + "_" + year + "_" + hallName + "_" + courseId + "_" + courseName
						+ "_" + capacity + "_" + enrollment);
				word.set(semesterHallCourseKey.toString());
				context.write(word, new IntWritable(1));
			} catch (NumberFormatException e) {
				return;
			}

		}
	}

	public static class Reducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			context.write(key, new IntWritable(1));
		}
	}

	public static class Mapper2 extends Mapper<Object, Text, Text, IntWritable> {
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String prevKey = value.toString().split("\\t")[0];
			prevKey = prevKey.trim();
			String[] prevKeyTerms = prevKey.split("_");
			String year = prevKeyTerms[1];
			String hallName = prevKeyTerms[2];
			StringBuilder hallYearKey = new StringBuilder();
			hallYearKey.append(year + " " + hallName);
			word.set(hallYearKey.toString());
			context.write(word, new IntWritable(1));
		}

	}

	public static class Reducer2 extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			this.result.set(sum);
			context.write(key, this.result);
		}

	}

}
