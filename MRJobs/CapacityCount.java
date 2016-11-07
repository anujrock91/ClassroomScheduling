
/*
 * Decompiled with CFR 0_114.

 * 
 * Could not load the following classes:
 *  org.apache.hadoop.conf.Configuration
 *  org.apache.hadoop.fs.Path
 *  org.apache.hadoop.io.IntWritable
 *  org.apache.hadoop.io.Text
 *  org.apache.hadoop.mapreduce.Job
 *  org.apache.hadoop.mapreduce.Mapper
 *  org.apache.hadoop.mapreduce.Mapper$Context
 *  org.apache.hadoop.mapreduce.Reducer
 *  org.apache.hadoop.mapreduce.Reducer$Context
 *  org.apache.hadoop.mapreduce.lib.input.FileInputFormat
 *  org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
 *  org.apache.hadoop.util.GenericOptionsParser
 */



//anujrast nalinkum TEAM

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
import org.apache.hadoop.util.GenericOptionsParser;

public class CapacityCount {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: CapacityCount <in> [<in>...] <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "word count");
		job.setJarByClass((Class) CapacityCount.class);
		job.setMapperClass((Class) TokenizerMapper.class);
		job.setCombinerClass((Class) IntSumReducer.class);
		job.setReducerClass((Class) IntSumReducer.class);
		job.setOutputKeyClass((Class) Text.class);
		job.setOutputValueClass((Class) IntWritable.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath((Job) job, (Path) new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath((Job) job, (Path) new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
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

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] terms = value.toString().split(",");
			terms[2] = terms[2].trim();

			try {
				int capacity = Integer.parseInt(terms[7]);
				StringBuilder year = new StringBuilder();
				String[] roomName = terms[2].split(" ");

				// data cleaning code
				if (roomName.length == 0) {
					return;
				} else {
					roomName[0] = roomName[0].trim();
					if (roomName[0].equalsIgnoreCase("Unknown") || roomName[0].equalsIgnoreCase("Arr")) {
						return;
					}

				}

				year.append(roomName[0] + "_" + terms[1]);
				word.set(year.toString());
				context.write(word, new IntWritable(capacity));
			} catch (NumberFormatException e) {
				return;
			}

		}
	}

}
