import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.google.common.collect.Iterables;

public class Statistics {

	public static String kCount = "count";
	public static String kMean = "mean";
	public static String kSD = "sd";
	public static String kMin = "min";
	public static String kMax = "max";
	public static String kP = "k25";
	public static String kPP = "k50";
	public static String kPPP = "k75";

	public static class Map extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private Text word = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			int number = Integer.parseInt(line);
			IntWritable number_i = new IntWritable(number);

			word.set(kCount);
			context.write(word, number_i);
			word.set(kMax);
			context.write(word, number_i);
			word.set(kMin);
			context.write(word, number_i);
			word.set(kMean);
			context.write(word, number_i);
			word.set(kP);
			context.write(word, number_i);
			word.set(kPP);
			context.write(word, number_i);
			word.set(kPPP);
			context.write(word, number_i);
			
			
		}
	}

	public static class Reduce extends
			Reducer<Text, IntWritable, Text, FloatWritable> {

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			String command = key.toString();
//			Iterators.size((Iterator<IntWritable>) values);
			if (command.equalsIgnoreCase(kCount)) {
				float count = 0;
				float sum = 0;
				
				for (IntWritable val : values) {
					count += 1;
					sum += val.get();
				}
				Text key2 = new Text(kMean);
				float mean = sum/count;
				context.write(key2, new FloatWritable(mean));
				context.write(key, new FloatWritable(count));
				
				float sdSum = 0;
				for (IntWritable val : values) {
					 sdSum += Math.pow(val.get()-mean, 2);
				}
				
				float sd = (float) Math.sqrt((float)(sdSum/count));
				Text keySD = new Text(kSD);
				context.write(keySD, new FloatWritable(count));

			} else if (command.equalsIgnoreCase(kMax)) {
				int max = Iterables.get(values, 0).get();
				for (IntWritable val : values) {
					max = max > val.get() ? max : val.get();
				}
				context.write(key, new FloatWritable(max));
			} else if (command.equalsIgnoreCase(kMin)) {
				IntWritable minI = Iterables.get(values, 0);
				int min = minI.get();
				for (IntWritable val : values) {
					min = min < val.get() ? min : val.get();
				}
				context.write(key, new FloatWritable(min));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		Job job = new Job(conf, "statistics");

		job.setJarByClass(Statistics.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		job.waitForCompletion(true);
	}

}