package apriori;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Generates Frequent Item Sets for current iteration
 *
 * @author niki
 *
 */
public class FrequentItemSetGen {
	
	// Global counter to keep track if there were any frequent item set (genre set) generated
	private enum COUNTERS {
		STOP

	}
	/**
	 * Reads DataSet file and counts the frequency of genre sets
	 *
     * @author niki
     *
     */
	public static class FrequentItemSetGenMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		/**
		 * HashMap object used to store genre sets and their corresponding frequencies
		 * which achieves task level combining 
		 */
		HashMap<String, Integer> genre_count;

		// Initializes HashMap object per Map task
		public void setup(Context context) throws IOException {
			genre_count = new HashMap<String, Integer>();
		}

		// Reads the DataSet file and stores genre sets and their corresponding frequencies 
		// in the HashMap object
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			StringTokenizer itr = new StringTokenizer(value.toString(), "\t");
			itr.nextToken();
			String genre_set;
			int count;

			// Iterates over the genre sets and counts their frequency, stores in Map object
			while (itr.hasMoreTokens()) {
				genre_set = itr.nextToken();
				if (genre_count.containsKey(genre_set)) {
					count = genre_count.get(genre_set);
					genre_count.put(genre_set, count + 1);
				} else
					genre_count.put(genre_set, 1);
			}
		}

		//Emits each genre set and it's count calculated per Map task
		public void cleanup(Context context) throws IOException,
				InterruptedException {

			for (String genre : genre_count.keySet()) {
				context.write(new Text(genre),
						new IntWritable(genre_count.get(genre)));
			}
		}
	}
	
	/**
	 * Reads DataSet file and counts the frequency of genre sets
	 *
     * @author niki
     *
     */
	public static class FrequentItemSetGenReducer extends
			Reducer<Text, IntWritable, Text, Text> {

		double threshold;

		// Fetches threshold (min support count) value from global counter
		public void setup(Context context) throws IOException,
				InterruptedException {

			String threshold_str = context.getConfiguration().get("threshold");
			threshold = Double.parseDouble(threshold_str);
		}

		// calculates frequency of each genre set globally
		public void reduce(Text key, Iterable<IntWritable> value,
				Context context) throws IOException, InterruptedException {
			int count = 0;
			for (IntWritable i : value) {
				count += i.get();
			}
			
			int retval = Double.compare((double) count, threshold);
			// If the frquency of genre set passes the threshold i.e. min support count then 
			// it is emitted. If there exits such genre set then the counter is incremented.
			if (retval >= 0) {
				context.getCounter(COUNTERS.STOP).increment(1L);
				context.write(key, null);
			}
		}
	}

	// Method that configures the job
	int configureFrequentItemSetGenJob(Configuration conf, String input,
			String output, double threshold) throws IOException,
			InterruptedException, ClassNotFoundException {
		
		// Set NLineInputFormat to send small file to multiple mappers
		conf.setInt(NLineInputFormat.LINES_PER_MAP, 10000);
		Job job = new Job(conf, "Frequent ItemSet Generation");
		
		job.setJarByClass(FrequentItemSetGen.class);
		
		job.setInputFormatClass(NLineInputFormat.class);
		
		job.setMapperClass(FrequentItemSetGenMapper.class);
		
		// Set number of reduce tasks
		job.setNumReduceTasks(15);
		
		job.setReducerClass(FrequentItemSetGenReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		// Indicates whether the job finished it's execution
		// exitStatus hold 0 if the job is completed otherwise 1
		int exitStatus = job.waitForCompletion(true) ? 0 : 1;

		// If the job is successfully completed and the counter value did not change then we did not 
		// find any frequent genre set and we return status 2 or return the exitStatus itself
		if (exitStatus == 0
				&& job.getCounters().findCounter(COUNTERS.STOP).getValue() == 0)
			return 2;

		return exitStatus;
	}

}
