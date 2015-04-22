import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Statistics {
	/**
	 * Z variables to calculate the quantiles Picked from the Z score table
	 * For more information look into Readme.
	 * Note :
	 * http://en.wikipedia.org/wiki/Standard_score#Calculation_from_raw_score
	 */
	private static final double Z25 = -0.675;
	private static final double Z50 = 0;
	private static final double Z75 = 0.675;

	public static class Map extends
			Mapper<LongWritable, Text, Text, MapWritable> {
		Text countKey = new Text("count");
		Text maxKey = new Text("max");
		Text minKey = new Text("min");
		Text sumKey = new Text("sum");
		Text textKey = new Text("1");

		MapWritable mw = new MapWritable();

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			int number = Integer.parseInt(line);
			IntWritable num = new IntWritable(number);

			mw.put(countKey, new IntWritable(1));
			mw.put(maxKey, new IntWritable(number));
			mw.put(minKey, new IntWritable(number));
			mw.put(sumKey, new IntWritable(number));

			context.write(textKey, mw);
		}
	}

	public static class Reduce extends
			Reducer<Text, MapWritable, Text, FloatWritable> {

		Text countKey = new Text("count");
		Text maxKey = new Text("max");
		Text minKey = new Text("min");
		Text sumKey = new Text("sum");
		Text textKey = new Text("1");
		MapWritable mw = new MapWritable();

		@Override
		protected void reduce(Text key, Iterable<MapWritable> values,
				Context context) throws IOException, InterruptedException {

			MapWritable firstMapWritable = values.iterator().next();
			int max = ((IntWritable) firstMapWritable.get(maxKey)).get();
			int min = ((IntWritable) firstMapWritable.get(minKey)).get();
			int count = ((IntWritable) firstMapWritable.get(countKey)).get();
			int number = ((IntWritable) firstMapWritable.get(sumKey)).get();

			int sum = number;

			int mean = 0;
			int M2 = 0;
			int delta = number - mean;
			mean = mean + delta / count;
			M2 += delta * (number - mean);

			for (MapWritable m : values) {
				IntWritable sumWritable = (IntWritable) m.get(sumKey);
				IntWritable countIntWritable = (IntWritable) m.get(countKey);
				// Calculating Standard deviation using algorithm proposed by Donald E. Knuth
				delta = number - mean;
				mean = mean + delta / count;
				M2 += delta * (number - mean);

				count += countIntWritable.get();

				IntWritable maxWritable = (IntWritable) m.get(maxKey);
				max = Math.max(maxWritable.get(), max);

				IntWritable minWritable = (IntWritable) m.get(minKey);
				min = Math.min(minWritable.get(), min);

				number = sumWritable.get();
				sum += number;
			}

			context.write(countKey, new FloatWritable(count));
			context.write(maxKey, new FloatWritable(max));
			context.write(minKey, new FloatWritable(min));
			float finalMean = (float) sum / count;
			context.write(new Text("mean"), new FloatWritable(finalMean));
			double sd = Math.sqrt((float) M2 / (count - 1));
			context.write(new Text("sd"), new FloatWritable((float) sd));

			/**
			 * Using Z score, (Standard score)
			 */
			double twentyfifth = finalMean + sd * Z25;
			double fiftyth = finalMean + sd * Z50;
			double seventyfifth = finalMean + sd * Z75;

			context.write(new Text("25th"), new FloatWritable(
					(float) twentyfifth));
			context.write(new Text("50th"), new FloatWritable((float) fiftyth));
			context.write(new Text("75th"), new FloatWritable(
					(float) seventyfifth));
		}
	}

	/**
	 * @param args
	 * @throws IOException
	 * @throws IllegalArgumentException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	public static void main(String[] args) throws IllegalArgumentException,
			IOException, ClassNotFoundException, InterruptedException {

		System.out.println(args[0]);
		System.out.println(args[1]);

		Path pt = new Path(args[2]);
		FileSystem fs = FileSystem.get(new Configuration());

		if (fs.exists(pt)) {
			fs.delete(pt, true);
			System.out.print("Deleted output directory for recreation..");
		}

		Configuration conf = new Configuration();

		Job job = new Job(conf, "statistics");

		job.setJarByClass(Statistics.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MapWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		job.waitForCompletion(true);
	}
}