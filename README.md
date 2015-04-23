# Fun Stats:

###YouTube Video Link:
https://youtu.be/DJxEoOl3Dkg

##Info:
Here we are implementing MAP REDUCE on STATISTIC functions like Count,Max,Min,Mean,Standard deviation,25th,50th and 70th percentile.

##Requirements:
IBM BigInsights

# Implementation:
Step 1: Open the eclipse in BigInsights.

Step 2: Get the FunStats code in eclipse.

Step 1: Statistics.Java has the required code for iplementaion.

Step 2: Get the jar file for stats.

Step 3: If you want to give input as some random variables,you can execute DataGenerator.java file by providing limit to the numbers.(Ex: data<10000000)

Step 3: Open BigInsights terminal and go the path containing the package FunStats

Step 4: Execute the following command

        >hadoop jar stats.jar Statistics InputPath OutputPath.

        Ex: >hadoop jar stats.jar Statistics Statistics_Input/ Output_logs/
        Here our Input file path is in Statistics_Input and Output file path is Output_logs

        Note : There is no need to delete the output path every time you run as FunStats will automatically do that for you.

Step 5: Wait for Map Reduce to implement.

Step 6: Check the output file in Hadoop File Broser.

<<<<<<< HEAD
=======
# Resources:
-- http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/FileSystemShell.html
>>>>>>> origin/master

# DataGenerator:

        Path pt = new Path(
		"hdfs://bivm.ibm.com:9000/user/biadmin/Statistics_Input/test.txt");
		FileSystem fs = FileSystem.get(new Configuration());

		BufferedWriter br = null;
		
		if (fs.exists(pt)) {
			br = new BufferedWriter(new OutputStreamWriter(
					fs.append(pt)));
		} else {
			br = new BufferedWriter(new OutputStreamWriter(
					fs.create(pt, true)));
		}
		
		// TO append data to a file, use fs.append(Path f)
		int flag = 1;
		while (flag <= COUNT) {
			int number = (int) (Math.random() * 20);
			br.append(""+number+"\n");
			flag++;
		}

# MapReduce:
The MapReduce implements a Multi-machine platform for programming using the the Google MapReduce idiom. Users specify a map function that processes a key/value pair to generate a set of intermediate key/value pairs, and a reduce function that merges all intermediate values associated with the same intermediate key.


# MapFunction:
map (k1,v1) --> list(k2,v2)

Map function gets input a key,value pair and generates a list of keys and its associated values.

Code:

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

# ReduceFunction:
reduce (k2,list(v2)) --> list(v2)

Reduce function gets input form map function ,and gives output as list of values.

<<<<<<< HEAD
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


            
# Resources:

http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/FileSystemShell.html

http://en.wikipedia.org/wiki/Standard_score   - Finding percentile.
Formula : Value = Mean + (Z value for percentile)*Standard_Deviation.

http://www.pindling.org/Math/Learning/Statistics/z_scores_table.htm    - Z table for Percentile.
=======
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
>>>>>>> origin/master
