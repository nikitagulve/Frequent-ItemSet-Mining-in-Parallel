# Frequent-ItemSet-Mining-in-Parallel
The idea of this project is to build a Music Recommendation System to recommend genres to users. It uses Frequent Itemset Mining algorithm,  Apriori, to achieve this. It makes use of MapReduce framework to achieve Parallel Data Processing. The goal is to find frequently occuring genre sets in Yahoo! Music Dataset. So that if a user is listening to one of the genres from a frequent genre set then the system would recommend other genres from the set to the user. Yahoo! Music Data Set represents a snapshot of the Yahoo! Music community's preferences for various songs. The dataset contains over 717 million ratings of 136 thousand songs given by 1.8 million users of Yahoo! Music services. The data was collected between 2002 and 2006. Each song in the data set is accompanied by artist, album, and genre attributes. The mapping from genres id's to genres, as well as the genre hierarchy, is given. The challenging part is to make use of inherent capabilites of Hadoop framework to implement an iterative algorithm. 

More details of implementation...

The file GenreSetGeneration.java performs a preprocessing step to find all the genres associated with users. Since, the data is in the form of two files: users mapped with songs and songs mapped with genres, a Join operation is performed to get the genres of corresponding users. 

(Note: The term Itemset corresponds to Genreset.)

The implementation has two parts: 

  -> The first part is to calculate Candidate Itemsets of size 2. This is done in file AprioriItemSet2.java. Given a Dataset      which is in the form of Itemsets of size 1, we use MapReduce's Join ability to form Itemsets of size 2. 
  
  -> The second part iteratively calculates Itemsets of size k (k>2). This is done using components: DataSetGen.java,             FrequentItemSetGen.java, CandidateSetGen.java, AprioriItemSetK.java. 
     
     Every kth iteration is as follows:
     
     1. Given a Candidate Itemset of size k (obtained from previous iteration), Dataset of size k is generated (users mapped to corresponding Itemsets of size k). Implemented in DataSetGen.java.
     2. Take frequency count of each Itemset and generate Candidate Itemsets of size k that pass the min support. Used Task Level Combining for optimization. Implemented in FrequentItemSetGen.java.
     3. Generate Candidate Itemsets of size k+1 by performing Secondary Sort. Implemented in CandidateSetGen.java. The results will be input to next iteration.
     
     The iterations will continue till no Frequent Itemsets are produced that pass min support.
     
The file AprioriItemSetK.java contains driver program (main method) that drives all the jobs in iteration.
     
The calculation of Datasets in every iteration is an overhead to achieve Pruning. This technique helps to achieve this task in parallel which is daunting otherwise.

Reference: http://www.rsrikant.com/papers/vldb94.pdf
  
  
  
     
    
