
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TimeSlotUtilization {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if (args.length != 3) {
			System.err.println("TYPE:   hadoop jar <JARNAME> input o1 o2");
			System.exit(2);
		}
		Configuration conf = new Configuration();

		Job job = new Job(conf, "enrollment and capacity with hall coursename time slot in different semesters");
		job.setJarByClass((Class) TimeSlotUtilization.class);
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
		Job job2 = new Job(conf2, "time slot utilization in different years");
		job2.setJarByClass((Class) TimeSlotUtilization.class);
		job2.setMapperClass((Class) Mapper2.class);
		job2.setReducerClass((Class) Reducer2.class);
		job2.setMapOutputKeyClass((Class) Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass((Class) Text.class);
		job2.setOutputValueClass(FloatWritable.class);
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
			terms[4] = terms[4].trim();
			terms[5] = terms[5].trim();
			terms[6] = terms[6].trim();
			terms[7] = terms[7].trim();
			terms[8] = terms[8].trim();
			StringBuilder semesterHallTimeSlotKey = new StringBuilder();
			String[] roomName = terms[2].split(" ");
			String[] semester = terms[1].split(" ");
			String year = semester[1];
			String season = semester[0];
			String hallName = roomName[0];
			String day = terms[3];
			String time = terms[4];
			String courseId = terms[5];
			String courseName = terms[6];
			String enrollMent = terms[7];
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

				Integer yearVal = Integer.parseInt(year);
				if (yearVal < 2014 || yearVal > 2016) {
					return;
				}

				Integer enrollMentVal = Integer.parseInt(enrollMent);
				Integer capacityVal = Integer.parseInt(capacity);

				// To visualize increase in seat utilization we neglect those
				// entries for which enrollment is greater than capacity
				if (enrollMentVal > capacityVal) {
					return;
				}

				if (day.equalsIgnoreCase("UNKWN") || day.equalsIgnoreCase("ARR")) {
					return;
				}

				if (time.equalsIgnoreCase("Unknown")) {
					return;
				}

				semesterHallTimeSlotKey.append(season + "_" + year + "_" + hallName + "_" + capacity + "_" + enrollMent
						+ "_" + day + "_" + time + "_" + courseId + "_" + courseName);
				word.set(semesterHallTimeSlotKey.toString());
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

	public static class Mapper2 extends Mapper<Object, Text, Text, Text> {
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			try {
				String[] prevRow = value.toString().split("\\t");
				String prevKey = prevRow[0];
				String[] prevKeyTerms = prevKey.split("_");
				String year = prevKeyTerms[1];
				String hallName = prevKeyTerms[2];
				// int capacity = Integer.parseInt(prevKeyTerms[3]);
				// int enrollMent = Integer.parseInt(prevKeyTerms[4]);
				String capacity = prevKeyTerms[3];
				String enrollMent = prevKeyTerms[4];
				String day = prevKeyTerms[5];
				String time = prevKeyTerms[6];
				StringBuilder YearTimeSlotKey = new StringBuilder();
				StringBuilder capEnrollValue = new StringBuilder();
				YearTimeSlotKey.append(year + "_" + day + "_" + time);
				word.set(YearTimeSlotKey.toString());
				capEnrollValue.append(enrollMent + "_" + capacity);
				context.write(word, new Text(capEnrollValue.toString()));
			} catch (NumberFormatException e) {
				return;
			}
		}

	}

	public static class Reducer2 extends Reducer<Text, Text, Text, FloatWritable> {
		private FloatWritable result = new FloatWritable();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			try {
				float sum1 = 0;
				float sum2 = 0;
				for (Text val : values) {
					String value = val.toString();
					value = value.trim();
					String[] capEnrollValues = value.split("_");
					int enrollMent = Integer.parseInt(capEnrollValues[0]);
					int capacity = Integer.parseInt(capEnrollValues[1]);
					sum1 = sum1 + enrollMent;
					sum2 = sum2 + capacity;
				}

				float utilizationPercentage = (((sum1 / sum2) * 100));
				this.result.set(utilizationPercentage);
				context.write(key, this.result);
			} catch (NumberFormatException e) {
				return;
			}
		}

	}

}
