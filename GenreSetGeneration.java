package apriori;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * This class is used to get all items(genreID) associated with a customer/transaction(userID)
 * 
 * @author niki
 *
 */
public class GenreSetGeneration {
	
	/**
	 * Mapper class that reads the song attributes file from Distributed cache 
	 * and reads the Ratings file with UserID and SongID and perform join to get all genres
	 * associated with the user.
	 * 
	 * @author niki
	 *
	 */
	 	
	public static class GenreMapper extends
			Mapper<Object, Text, IntPair, IntWritable> {
		HashMap<String, String> genresBySong;

		BufferedReader buffer;
		
		//Constants
		int USERIDINDEX = 0;
		int SONGIDINDEX = 1;
		String SONGATTRIBUTESFILE = "song-attributes.txt";
		int SONGIDINDEXINCACHE = 0;
		int GENREIDINDEXINCACHE = 3;
		
		// Initiate the map and get the songs file from cache
		protected void setup(Context context) {
			genresBySong = new HashMap<String, String>();
			try {
				Path[] cacheFilesLocal = DistributedCache
						.getLocalCacheFiles(context.getConfiguration());
				for (Path eachPath : cacheFilesLocal) {
					if (eachPath.getName().toString().trim()
							.equals(SONGATTRIBUTESFILE)) {
						analyseFile(eachPath, context);
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		// Read all the files from the distributed cache and stores it into HashMap
		private void analyseFile(Path file, Context context) throws IOException {
			String str = "";
			try {
				buffer = new BufferedReader(new FileReader(file.toString()));
				while ((str = buffer.readLine()) != null) {
					String songFields[] = str.split("\\t");
					genresBySong.put(songFields[SONGIDINDEXINCACHE], songFields[GENREIDINDEXINCACHE]);
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				if (buffer != null) {
					buffer.close();
				}
			}
		}
		
		/* 
		 * Map takes each record from file and matches it with the song genreID from the Hashmap
		 * It emits the UserID, genreID as key and genreID as value.
		 */
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] line = value.toString().split("\\t");
			if (line[SONGIDINDEX] != null && genresBySong.containsKey(line[SONGIDINDEX])) {
				
				int genre = Integer.parseInt(genresBySong.get(line[SONGIDINDEX]));
				IntPair keyPair = new IntPair();
				keyPair.set(Integer.parseInt(line[USERIDINDEX]), genre);

				context.write(keyPair, new IntWritable(genre));
			}
		}
	}
	
	/**
	 * Reducer class gets the key with UserID, genreID with values sorted by genreID and emits all the genres
	 * of the userID in ascending order.
	 * 
	 * @author niki
	 */
	public static class GenreReducer extends Reducer<IntPair, IntWritable, IntWritable, Text> {

		public void reduce(IntPair key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			StringBuilder result = new StringBuilder();
			HashSet<String> genres = new HashSet<String>();
			int i = 1;
			for (IntWritable val : values) {
				if (!genres.contains(val.toString())) {
					if (i == 1)
						result.append(val.toString());
					else
						result.append(" ," + val.toString());
					genres.add(val.toString());
					i++;
				}
			}

			context.write(new IntWritable(key.first), new Text(result.toString()));
		}
	}
	
	/**
	 * Class that configures MR job
	 * 
	 * @author niki
	 */
	public static class DriverMapSideJoinDCacheTxtFile extends Configured
			implements Tool {
		
		//Constants
		String CACHEFILEPATH = "/Users/gulve.n/Documents/workspace/AprioriParallel/song-attributes.txt"; 
		int NUMREDUCETASKS = 10;
		
		@Override
		public int run(String[] args) throws Exception {

			if (args.length != 2) {
				System.out
						.printf("Two parameters are required- <input dir> <output dir>\n");
				return -1;
			}

			Job job = new Job(getConf());
			
			Configuration conf = job.getConfiguration();
			job.setJobName("Map-side join with text lookup file in DCache");
			
			// Add song file to distributed cache
			DistributedCache.addCacheFile(new URI(CACHEFILEPATH), conf);
			
			job.setJarByClass(GenreSetGeneration.class);

			// Set input and output path
			FileInputFormat.setInputPaths(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
			// Set Mapper and Reducer class
			job.setMapperClass(GenreMapper.class);
			job.setReducerClass(GenreReducer.class);
			
			// Set Map output key and value
			job.setMapOutputKeyClass(IntPair.class);
			job.setMapOutputValueClass(IntWritable.class);
			
			// Set output key and value
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(Text.class);
			
			// Set Number of reduce tasks
			job.setNumReduceTasks(NUMREDUCETASKS);

			boolean success = job.waitForCompletion(true);
			return success ? 0 : 1;
		}

	}

	public static void main(String[] args) throws Exception {

		int exitCode = ToolRunner.run(new Configuration(),
				new DriverMapSideJoinDCacheTxtFile(), args);
		System.exit(exitCode);
	}

	/**
	 * Define a pair of integers that are writable. They are serialized in a
	 * byte comparable format.
	 * 
	 * @author niki
	 *
	 */
	public static class IntPair implements WritableComparable<IntPair> {
		private int first = 0;
		private int second = 0;

		/**
		 * Set the left and right values.
		 */
		public void set(int left, int right) {
			first = left;
			second = right;
		}

		public int getFirst() {
			return first;
		}

		public int getSecond() {
			return second;
		}

		/**
		 * Read the two integers. Encoded as: MIN_VALUE -> 0, 0 -> -MIN_VALUE,
		 * MAX_VALUE-> -1
		 */
		@Override
		public void readFields(DataInput in) throws IOException {
			first = in.readInt() + Integer.MIN_VALUE;
			second = in.readInt() + Integer.MIN_VALUE;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(first - Integer.MIN_VALUE);
			out.writeInt(second - Integer.MIN_VALUE);
		}

		@Override
		public int hashCode() {
			return first * 157 + second;
		}

		@Override
		public boolean equals(Object right) {
			if (right instanceof IntPair) {
				IntPair r = (IntPair) right;
				return r.first == first && r.second == second;
			} else {
				return false;
			}
		}

		/** A Comparator that compares serialized IntPair. */
		public static class Comparator extends WritableComparator {
			public Comparator() {
				super(IntPair.class);
			}

			public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2,
					int l2) {
				return compareBytes(b1, s1, l1, b2, s2, l2);
			}
		}

		static { // register this comparator
			WritableComparator.define(IntPair.class, new Comparator());
		}

		@Override
		public int compareTo(IntPair o) {
			if (first != o.first) {
				return first < o.first ? -1 : 1;
			} else if (second != o.second) {
				return second < o.second ? -1 : 1;
			} else {
				return 0;
			}
		}
	}

	/**
	 * Partition based on the first part of the pair.
	 */
	public static class FirstPartitioner extends
			Partitioner<IntPair, IntWritable> {
		@Override
		public int getPartition(IntPair key, IntWritable value,
				int numPartitions) {
			return Math.abs(key.getFirst() * 127) % numPartitions;
		}
	}

	/**
	 * Compare only the first part of the pair, so that reduce is called once
	 * for each value of the first part.
	 */
	public static class FirstGroupingComparator implements
			RawComparator<IntPair> {
		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return WritableComparator.compareBytes(b1, s1, Integer.SIZE / 8,
					b2, s2, Integer.SIZE / 8);
		}

		@Override
		public int compare(IntPair o1, IntPair o2) {
			int l = o1.getFirst();
			int r = o2.getFirst();
			return l == r ? 0 : (l < r ? -1 : 1);
		}
	}

}
