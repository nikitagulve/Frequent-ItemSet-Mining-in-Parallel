package apriori;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Class to find the candidate Item Sets of size 2.
 * 
 * @author niki
 * 
 */
public class AprioriItemSet2 {
	
	/**
	 * Frequent ItemSet Mapper class reads the records from input file which consists of User IDs and their 
	 * corresponding generes (set size 1) and emits count of each genre
	 *
	 * @author niki
	 *
	 */
	public static class FrequentItemSetGenMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		HashMap<String, Integer> genre_count;

		//set up a map variable to perform form task level combining
		public void setup(Context context) throws IOException,
				InterruptedException {
			genre_count = new HashMap<String, Integer>();
		}

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			StringTokenizer itr = new StringTokenizer(value.toString(), "\t");
			//Skip the first token, i.e. user id
			itr.nextToken();
			String genre;
			int count;
			while (itr.hasMoreTokens()) {
				genre = itr.nextToken();
				if (genre_count.containsKey(genre)) {
					count = genre_count.get(genre);
					genre_count.put(genre, count + 1);
				} else
					genre_count.put(genre, 1);
			}
		}

		public void cleanup(Context context) throws IOException,
				InterruptedException {
			//emit aggregated count at task level of genres
			for (String genre : genre_count.keySet()) {
				context.write(new Text(genre),
						new IntWritable(genre_count.get(genre)));
			}
		}
	}

	/**
	 * Frequent Itemset Reducer class that calculates frequency of genres and emits only those that pass the 
	 * min support count.
	 *
	 * @author niki
	 *
	 */
	public static class FrequentItemSetGenReducer extends
			Reducer<Text, IntWritable, Text, Text> {
		
		// min support count
		double threshold;

		public void setup(Context context) throws IOException,
				InterruptedException {
			//Retrieve min support count from Configuration
			String threshold_str = context.getConfiguration().get("threshold");
			threshold = Double.parseDouble(threshold_str);
		}

		// get count of genres and emit the ones that pass threshold
		public void reduce(Text key, Iterable<IntWritable> value,
				Context context) throws IOException, InterruptedException {
			int count = 0;
			for (IntWritable i : value) {
				count += i.get();
			}
			int retval = Double.compare((double) count, threshold);
			if (retval >= 0)
				context.write(key, null);
		}
	}

	/**
	 * Candidate Itemset Mapper class that emits a dummy key and the frequent genres
	 *
	 * @author niki
	 *
	 */
	public static class CandidateSet2GenMapper extends
			Mapper<Object, Text, KeyPair, Text> {
		//emit the records with composite key where the first part is a dummy key
		//and the second part is genre id 
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			KeyPair keyPair = new KeyPair("reduce", Integer.parseInt(value
					.toString()));
			context.write(keyPair, value);

		}
	}

	/**
	 * Candidate ItemSet Reducer class computes all combinations of genre pairs
	 *
	 * @author niki
	 *
	 */
	public static class CandidateSet2GenReducer extends
			Reducer<KeyPair, Text, Text, Text> {
		
		//genres are received in sorted order, compute possible candidates of size 2
		//and emit them
		public void reduce(KeyPair key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			StringBuilder candidate;
			List<String> prev = new ArrayList<String>();
			for (Text value : values) {
				for (int i = 0; i < prev.size(); i++) {
					candidate = new StringBuilder();
					candidate.append(prev.get(i));
					candidate.append(",");
					candidate.append(value);
					context.write(new Text(candidate.toString()), null);
				}
				prev.add(value.toString());
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}

		conf.setInt(NLineInputFormat.LINES_PER_MAP, 100000);
		conf.set("threshold", args[2]);
		
		// Job to Get the frequent Itemset of Size 1 based on threshold
		Job frequentItemSet1Job = new Job(conf, "Frequent ItemSet1 ");
		frequentItemSet1Job.setInputFormatClass(NLineInputFormat.class);
		
		frequentItemSet1Job.setJarByClass(AprioriItemSet2.class);
		
		frequentItemSet1Job.setMapperClass(FrequentItemSetGenMapper.class);
		frequentItemSet1Job.setReducerClass(FrequentItemSetGenReducer.class);
		
		// Set number of reduce tasks
		frequentItemSet1Job.setNumReduceTasks(15);
		
		frequentItemSet1Job.setOutputKeyClass(Text.class);
		frequentItemSet1Job.setOutputValueClass(Text.class);
		
		frequentItemSet1Job.setMapOutputKeyClass(Text.class);
		frequentItemSet1Job.setMapOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(frequentItemSet1Job, new Path(otherArgs[1]
				+ "DataSet1"));
		FileOutputFormat.setOutputPath(frequentItemSet1Job, new Path(
				otherArgs[1] + "Frequent1"));
		
		// Job to create Candidate itemset of size 2 using the frequent itemset of size 1
		conf.setInt(NLineInputFormat.LINES_PER_MAP, 1000);
		Job candidateSet2GenJob = new Job(conf, "CandidateSet2 Gen");
		candidateSet2GenJob.setInputFormatClass(NLineInputFormat.class);
		
		candidateSet2GenJob.setJarByClass(AprioriItemSet2.class);
		
		candidateSet2GenJob.setMapperClass(CandidateSet2GenMapper.class);
		candidateSet2GenJob.setReducerClass(CandidateSet2GenReducer.class);
		
		candidateSet2GenJob.setPartitionerClass(KeyPairPartitioner.class);
		
		candidateSet2GenJob.setSortComparatorClass(KeyPairComparator.class);
		
		candidateSet2GenJob
				.setGroupingComparatorClass(KeyPairGroupComparator.class);
		
		// Set number of reduce tasks
		candidateSet2GenJob.setNumReduceTasks(1);
		
		candidateSet2GenJob.setOutputKeyClass(Text.class);
		candidateSet2GenJob.setOutputValueClass(Text.class);
		
		candidateSet2GenJob.setMapOutputKeyClass(KeyPair.class);
		candidateSet2GenJob.setMapOutputValueClass(Text.class);
		
		// Set input and output path
		FileInputFormat.addInputPath(candidateSet2GenJob, new Path(otherArgs[1]
				+ "Frequent1"));
		
		FileOutputFormat.setOutputPath(candidateSet2GenJob, new Path(
				otherArgs[1] + "Candidates2"));
		
		System.exit(frequentItemSet1Job.waitForCompletion(true) ? (candidateSet2GenJob
				.waitForCompletion(true) ? 0 : 1) : 1);
	}

	/**
	 * Custom Class class to use for Secondary sort
	 * 
	 * @author niki
	 *
	 */
	@SuppressWarnings("rawtypes")
	public static class KeyPair implements WritableComparable {
		// The UniqueCarrier for Flight
		private String keyItems;

		// The month of the Flight
		private int lastItem;

		public KeyPair() {

		}

		public String getKeyItems() {
			return keyItems;
		}

		public int getLastItem() {
			return lastItem;
		}

		public KeyPair(String keyItems, int lastItem) {

			this.keyItems = keyItems;
			this.lastItem = lastItem;
		}

		public void write(DataOutput out) throws IOException {

			WritableUtils.writeString(out, keyItems);
			out.writeInt(lastItem);
		}

		/**
		 * Read the carrier and month.
		 */
		public void readFields(DataInput in) throws IOException {

			keyItems = WritableUtils.readString(in);
			lastItem = in.readInt();
		}

		/**
		 * Override compareTo of to serialize CarrierAndDate
		 */
		public int compareTo(Object o) {

			KeyPair that = (KeyPair) o;
			int c = keyItems.compareTo(that.keyItems);

			if (c != 0)
				return c;

			if (lastItem == that.lastItem)
				return 0;

			return (lastItem > that.lastItem ? 1 : -1);

		}

		// Hash code for the class
		public int hashCode() {
			return (int) (keyItems.hashCode() + lastItem);
		}

		// Override equals to compare carrier and month for equality
		public boolean equals(Object o) {

			KeyPair other;
			if (o instanceof KeyPair) {

				other = (KeyPair) o;
			} else
				return false;

			return (keyItems.equalsIgnoreCase(other.keyItems) && (lastItem == other.lastItem));

		}

		public String toString() {

			StringBuilder sb = new StringBuilder();
			sb.append(keyItems);
			sb.append(lastItem);

			return sb.toString();

		}

	}

	/**
	 * Partitioner to partition based on the carrier in the key
	 * 
	 * @author niki
	 *
	 */
	public static class KeyPairPartitioner extends Partitioner<KeyPair, Text> {

		public int getPartition(KeyPair key, Text value, int numPartitions) {

			int hash = Math.abs(key.keyItems.hashCode());
			int partition = hash % numPartitions;

			return partition;

		}

	}

	/**
	 * Key comparator to compare two keys
	 * 
	 * @author niki
	 *
	 */
	public static class KeyPairComparator extends WritableComparator {

		protected KeyPairComparator() {
			super(KeyPair.class, true);
		}

		@SuppressWarnings("rawtypes")
		public int compare(WritableComparable k1, WritableComparable k2) {

			KeyPair ut1 = (KeyPair) k1;
			KeyPair ut2 = (KeyPair) k2;

			return ut1.compareTo(ut2);
		}
	}

	/**
	 * Compare only the carrier part of the key, so that reduce is called once
	 * for each value of the carrier.
	 */
	public static class KeyPairGroupComparator extends WritableComparator {

		protected KeyPairGroupComparator() {
			super(KeyPair.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable k1, WritableComparable k2) {

			KeyPair ut1 = (KeyPair) k1;
			KeyPair ut2 = (KeyPair) k2;

			return ut1.getKeyItems().compareTo(ut2.getKeyItems());

		}

	}

}
