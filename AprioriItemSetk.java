package apriori;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;

/**
 * Drives all the jobs, the jobs are executed in iterations, the iterations stop
 * once the frequent item sets have stopped generating 
 * 
 * @author niki
 */

public class AprioriItemSetk {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		 
		// Sets minimum support count (threshold)
		double threshold = Double.parseDouble(otherArgs[2]);
		int i = 2;
		
		// Sets a global counter for threshold
		conf.set("threshold", otherArgs[2]);
		
		// Computes Frequent ItemSets of size k where k increases iteratively. It involves 
		// following steps. I] Compute DataSet of size k. II] Compute Frequent ItemSets of 
		// size k. III] Compute Candidates of size k+1. Each step is a MR job and the step
		// proceeds based on status of previous step.
		while (true) {
			String path[] = { otherArgs[1] + "DataSet" + String.valueOf(i - 1),
					otherArgs[1] + "DataSet" + String.valueOf(i),
					otherArgs[1] + "Candidates" + String.valueOf(i) };

			int exitDataSetGen = ToolRunner.run(conf,
					new DataSetGen.DataSetGenDriver(), path);

			if (exitDataSetGen == 0) {
				int exitFrequentItemSetGen = new FrequentItemSetGen()
						.configureFrequentItemSetGenJob(conf, path[1], otherArgs[1]
								+ "Frequent" + String.valueOf(i), threshold);

				if (exitFrequentItemSetGen == 0) {

					int exitCandidateSetGen = new CandidateSetGen()
							.configureCandidateSetGenJob(conf, otherArgs[1]
									+ "Frequent" + String.valueOf(i), otherArgs[1]
									+ "Candidates" + String.valueOf(i + 1));
					if (exitCandidateSetGen == 1)
						System.exit(1);
				} else if (exitFrequentItemSetGen == 2) {
					break;
				} else
					System.exit(1);
			} else
				System.exit(1);
			i++;
		}
	}
}
