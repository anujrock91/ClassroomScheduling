
import java.io.IOException;
import java.util.ArrayList;
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

public class CourseCapacityIncrease {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if (args.length != 4) {
			System.err.println("TYPE:   hadoop jar <JARNAME> input o1 o2 o3");
			System.exit(2);
		}
		Configuration conf = new Configuration();

		Job job = new Job(conf, "building courseid coursename and enrollment in every semester");
		job.setJarByClass((Class) CourseCapacityIncrease.class);
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
		Job job2 = new Job(conf2, "course capacity in every year one key");
		job2.setJarByClass((Class) CourseCapacityIncrease.class);
		job2.setMapperClass((Class) Mapper2.class);
		job2.setReducerClass((Class) Reducer2.class);
		job2.setMapOutputKeyClass((Class) Text.class);
		job2.setMapOutputValueClass(IntWritable.class);
		job2.setOutputKeyClass((Class) Text.class);
		job2.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath((Job) job2, (Path) new Path(args[1].toString()));
		FileOutputFormat.setOutputPath((Job) job2, (Path) new Path(args[2].toString()));
		job2.waitForCompletion(true);

		Configuration conf3 = new Configuration();
		Job job3 = new Job(conf3, "course capacity increase in every year range");
		job3.setJarByClass((Class) CourseCapacityIncrease.class);
		job3.setMapperClass((Class) Mapper3.class);
		job3.setReducerClass((Class) Reducer3.class);
		job3.setMapOutputKeyClass((Class) Text.class);
		job3.setMapOutputValueClass(IntWritable.class);
		job3.setOutputKeyClass((Class) Text.class);
		job3.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath((Job) job3, (Path) new Path(args[2].toString()));
		FileOutputFormat.setOutputPath((Job) job3, (Path) new Path(args[3].toString()));
		System.exit(job3.waitForCompletion(true) ? 0 : 1);

	}

	public static class Mapper1 extends Mapper<Object, Text, Text, IntWritable> {
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws InterruptedException, IOException {
			String[] terms = value.toString().split(",");
			terms[1] = terms[1].trim();
			terms[2] = terms[2].trim();
			terms[3] = terms[3].trim();
			terms[5] = terms[5].trim();
			terms[6] = terms[6].trim();
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

				semesterHallCourseKey.append(season + "_" + year + "_" + hallName + "_" + enrollment + "_" + capacity
						+ "_" + courseId + "_" + courseName);
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
			try {
				String prevKey = value.toString().split("\\t")[0];
				prevKey = prevKey.trim();
				String[] prevKeyTerms = prevKey.split("_");
				String year = prevKeyTerms[1];
				String courseId = prevKeyTerms[5];
				String courseName = prevKeyTerms[6];
				int capacity = Integer.parseInt(prevKeyTerms[4]);
				StringBuilder courseYearKey = new StringBuilder();
				courseYearKey.append(year + "_" + courseId + "_" + courseName);
				word.set(courseYearKey.toString());
				context.write(word, new IntWritable(capacity));
			} catch (NumberFormatException e) {
				return;
			}
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

	public static class Mapper3 extends Mapper<Object, Text, Text, IntWritable> {
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			try {
				String[] input = value.toString().split("\\t");
				String prevKey = input[0];
				int capacity = Integer.parseInt(input[1]);
				prevKey = prevKey.trim();
				String[] prevKeyTerms = prevKey.split("_");
				String year = prevKeyTerms[0];
				String courseId = prevKeyTerms[1];
				String courseName = prevKeyTerms[2];

				StringBuilder courseYearKey1 = new StringBuilder();
				StringBuilder courseYearKey2 = new StringBuilder();
				courseYearKey1.append(Integer.toString(((Integer.parseInt(year)) - 1)) + "-" + year + "_" + courseId
						+ "_" + courseName);
				courseYearKey2.append(year + "-" + Integer.toString(((Integer.parseInt(year)) + 1)) + "_" + courseId
						+ "_" + courseName);

				word.set(courseYearKey1.toString());
				context.write(word, new IntWritable(capacity));

				word.set(courseYearKey2.toString());
				context.write(word, new IntWritable(capacity));
			} catch (NumberFormatException e) {
				return;
			}
		}

	}

	public static class Reducer3 extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			Iterator<IntWritable> iter = values.iterator();
			int value1 = iter.next().get();
			if (!iter.hasNext()) {
				return;
			}
			int value2 = iter.next().get();
			this.result.set(value2 - value1);
			context.write(key, this.result);

		}

	}

}
