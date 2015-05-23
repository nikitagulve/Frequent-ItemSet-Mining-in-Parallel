package apriori;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

/**
 * Creates _Ck (DataSet file) which has users and correponding genre sets of size k
 * 
 * @author niki
 */
public class DataSetGen {

	/**
	 * Emits genre sets of size k against users only if their (k-1) subsets are frequent
	 * 
     * @author niki
     */
	public static class DataSetGenMapper extends
			Mapper<Object, Text, Text, Text> {
		BufferedReader buffer;
		Set<String> candidates;
		List<String> genreSets = new ArrayList<String>();

		// load all the candidates in the candidates variable from distributed cache
		protected void setup(Context context) throws IOException {
			candidates = new HashSet<String>();
			try {
				Path[] cacheFilesLocal = DistributedCache
						.getLocalCacheFiles(context.getConfiguration());
				for (Path eachPath : cacheFilesLocal) {

					analyseFile(eachPath, context);

				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		// Stores all the candidates in the Set object candidates
		private void analyseFile(Path file, Context context) throws IOException {
			String str = "";
			try {
				buffer = new BufferedReader(new FileReader(file.toString()));
				while ((str = buffer.readLine()) != null) {
					candidates.add(str);
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

		// Emits candidates of size k against users  that have all their subsets k-1
		// present in _CK-1
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			StringTokenizer str = new StringTokenizer(value.toString(), "\t");
			String user = str.nextToken();
			Set<String> genre_set_data = new HashSet<String>();
			while (str.hasMoreTokens()) {
				genre_set_data.add(str.nextToken());
			}
			
			Set<String> all_combinations;
			for (String genre_set : candidates) {
				// Fetches all the k-1 subset combinations of genre set of size k
				all_combinations = new DataSetGen().getCombinations(genre_set);
				// Condition that checks if all the k-1 subsets of the candidate genre set 
				// of size k are present in DataSets of a single user. If it passes then the 
				// candidate set is emitted against the user
				if (genre_set_data.containsAll(all_combinations)) {
					context.write(new Text(user), new Text(genre_set));
				
				}
			}
		}
	}

	/**
	 * Emits genre sets of size k against users only if their (k-1) subsets are frequent
	 * 
     * @author niki
     */
	public static class DataSetGenReducer extends
			Reducer<Text, Text, Text, Text> {
		// Emits user and all the corresponding candidate sets that are frequent
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			StringBuilder sb = new StringBuilder();
			boolean first = true;
			for (Text value : values) {
				if (first)
					sb.append(value);
				else {
					sb.append("\t");
					sb.append(value);
				}
				first = false;;
			}
			context.write(key, new Text(sb.toString()));
		}
	}

	/**
	 * Class that configures MR job
	 * 
	 * @author niki
	 */
	public static class DataSetGenDriver extends Configured implements Tool {

		@Override
		public int run(String[] args) throws Exception {
			Configuration conf = getConf(); 
			conf.setInt(NLineInputFormat.LINES_PER_MAP,10000);
			Job job = new Job(conf);
		
			job.setJobName("Map-side join with text lookup file in DCache");
			
			URI uri = URI.create(args[2]);
			FileSystem fs = FileSystem.get(uri, job.getConfiguration());   
			
			FileStatus[] fileStatus = fs.listStatus(new Path(args[2]));
			// Using FileUtil, getting the Paths for all the FileStatus
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			//Iterate through the directory and display the files in it
			for (Path path : paths) {
				DistributedCache.addCacheFile(path.toUri(), job.getConfiguration());
			}

			job.setInputFormatClass(NLineInputFormat.class);
			job.setJarByClass(DataSetGen.class);
			FileInputFormat.setInputPaths(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			job.setMapperClass(DataSetGenMapper.class);
			job.setReducerClass(DataSetGenReducer.class);
			job.setNumReduceTasks(15);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			boolean success = job.waitForCompletion(true);
			return success ? 0 : 1;
		}

	}

	// Given a string, it generates all combinations of the contents of the String object
	Set<String> getCombinations(String str){
		String arr[] = str.split(",");
		// A temporary array to store all combination one by one
		String data[] = new String[arr.length - 1];

		// Print all combination using temprary array 'data[]'
		Set<String> set = new HashSet<String>();

		combinationUtil(arr, data, 0, arr.length - 1, 0, arr.length - 1, set);

		return set;
	}

	//Prints all possible combinations of elements in array
	void combinationUtil(String arr[], String data[], int start, int end,
			int index, int r, Set<String> set) {
		// Current combination is ready to be printed, print it
		if (index == r) {
			StringBuilder sb = new StringBuilder();

			for (int j = 0; j < r; j++) {
				if (j == 0) {
					sb.append(data[j]);
				} else {
					sb.append(",");
					sb.append(data[j]);
				}
			}
			set.add(sb.toString());
			return;
		}

		for (int i = start; i <= end && end - i + 1 >= r - index; i++) {
			data[index] = arr[i];
			combinationUtil(arr, data, i + 1, end, index + 1, r, set);
		}
	}

}
