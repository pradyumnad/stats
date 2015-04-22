# Stats:

#YouTube Video Link:
https://youtu.be/DJxEoOl3Dkg

# Info:
Here we are implementing MAP REDUCE on STATISTIC functions like Count,Max,Min,Mean,Standard deviation,25th,50th and 70th percentile.

# Requirements:
IBM BigInsights

# Implementation:
Step 1: Open the eclipse in BigInsights.

Step 2: Get the FunStats code in eclipse.

Step 1: Statistics.Java has the required code for iplementaion.

Step 2: Get the jar file for stats.

Step 3: Open BigInsights terminal and go the path containing the package FunStats

Step 4: Execute the following command

        >hadoop jar stats.jar Statistics InputPath OutputPath.

        Ex: >hadoop jar stats.jar Statistics Statistics_Input/ Output_logs/
        Here our Input file path is in Statistics_Input and Output file path is Output_logs

        Note : Make sure You are using unique Output file name(Which is not in the directory).

Step 5: Wait for Map Reduce to implement.

Step 6: Check the output file in Hadoop File Broser.

# Resources:
-- http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/FileSystemShell.html


# MapReduce:
The MapReduce implements a Multi-machine platform for programming using the the Google MapReduce idiom. Users specify a map function that processes a key/value pair to generate a set of intermediate key/value pairs, and a reduce function that merges all intermediate values associated with the same intermediate key.

# MapFunction:
map (k1,v1) --> list(k2,v2)

Map function gets input a key,value pair and generates a list of keys and its associated values.

Code:

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException 
        {
            String line = value.toString();
            int number = Integer.parseInt(line);
            IntWritable number_i = new IntWritable(number);

            word.set(kCount);
            context.write(word, number_i);
         }         

# ReduceFunction:
reduce (k2,list(v2)) --> list(v2)

Reduce function gets input form map function ,and gives output as list of values.

            public void reduce(Text key, Iterable<IntWritable> values,
                Context context) throws IOException, InterruptedException
            {
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
