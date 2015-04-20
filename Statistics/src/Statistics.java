import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Statistics {

    public static String kCount = "count";
    public static String kMean = "mean";
    public static String kSD = "standard deviation";
    public static String kMin = "min";
    public static String kMax = "max";
    public static String kP = "25";
    public static String kPP = "50";
    public static String kPPP = "75";

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
        }
    }

    public static class Reduce extends
            Reducer<Text, IntWritable, Text, FloatWritable> {

        public void reduce(Text key, Iterable<IntWritable> values,
                Context context) throws IOException, InterruptedException {
            String command = key.toString();
            List<IntWritable> cache = new ArrayList<IntWritable>();
            // Iterators.size((Iterator<IntWritable>) values);
            if (command.equalsIgnoreCase(kCount)) {
                float count = 0;
                float sum = 0;

                int max = 0;
                int min = max;
                
                for (IntWritable val : values) {
                    count += 1;
                    int n = val.get();
                    if(count == 1) {
                        min = n;
                        max = n;
                    }
                    max = max > n ? max : n;
                    min = min < n ? min : n;
                    sum += n;
                    cache.add(new IntWritable(n));
                }
                
                context.write(key, new FloatWritable(count));
                context.write(new Text("max"), new FloatWritable(max));
                context.write(new Text("min"), new FloatWritable(min));
                Text key2 = new Text(kMean);
                float mean = (float) sum / count;
                context.write(key2, new FloatWritable(mean));

                float sdSum = 0;
                for (IntWritable val : cache) {
                    float diff = val.get() - mean;
                    diff = diff * diff;
                    sdSum += diff;
                }

                float sd = (float) Math.sqrt((float) (sdSum / count));
                Text keySD = new Text(kSD);
                context.write(keySD, new FloatWritable(sd));

                
                float tfp = percentile(cache, count, 25);
                float fp = percentile(cache, count, 50);
                float sfp = percentile(cache, count, 75);

                context.write(new Text(kP), new FloatWritable(tfp));
                context.write(new Text(kPP), new FloatWritable(fp));
                context.write(new Text(kPPP), new FloatWritable(sfp));
            }
        }
        
        private float percentile(List<IntWritable> values, float count,
                int type) {
            float range = (float)(type / 100.0) * (count + 1);
            
            int IR = (int) Math.floor(range);
            float FR = range - IR;

            int val1 = values.get(IR-1).get();
            int val2 = values.get(IR).get();

            float percentile = FR * (val2 - val1) + val1;
            return percentile;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "stats");

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
