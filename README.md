solr-recommender
================

Recommender using Solr, works online by supplying user history as the query and so may contain history that is not yet in the training data. The result is generated quickly (depending on size of training data and scaling of Solr) and so can be used in online situations.  This job also can create all recommendations and item-item similarities for all users and items using the Mahout Recommender. It also has a cross-action recommender for cases when a secondary action can be used to recommend the primary action. For example the primary action is purchase but view data is also available and so can be used to recommend purchases, see Theory below. Does batch updates of the training/index data using Mahout.

## Getting Started

To compile the sources download, build, and install the lastest snapshot of Mahout from http://mahout.apache.org. Then go to the root of this project and run
```
~$ mvn install
```

This will build the job jar with all dependencies in ./target/solr-recommender-0.1-SNAPSHOT-job.jar. The various tasks inside the jar are described below. Each CLI accessible job comes with a fairly complete description of its parameters printed out by running with no parameters.

There is a sample script for running the job in scripts/solr-recommender. To get a trivial sample output go to the solr-recommender/scripts directory and run:
```
~$ ./solr-recommender
```
This will use simple sample data from the project and create sample output in project root dir.

Running any job will output help, for example:
```
~$ hadoop jar ../target/solr-recommender-0.1-SNAPSHOT-job.jar \
       finderbots.recommenders.hadoop.RecommenderUpdateJob
```
Prints the following:
```
 -a1 (--action1) VAL           : String respresenting action1, the primary
                                 preference action (optional). Default:
                                 'purchase'
 -a2 (--action2) VAL           : String respresenting action2, the secondary
                                 preference action (optional). Default: 'view'
 -ac (--actionColumn) N        : Which column has the action (optional).
                                 Default: 1
 -i (--input) VAL              : Input directory searched recursively for files
                                 in 'ExternalID' format where ID are unique
                                 strings and preference files contain combined
                                 actions with action IDs. Subdirs will be
                                 created by action type, so 'purchase', 'view',
                                 etc.
 -ifp (--inputFilePattern) VAL : Match this pattern when searching for action
                                 log files (optional). Default: '.tsv'
 -iidc (--itemIDColumn) N      : Which column has the itemID (optional).
                                 Default: 2
 -ix (--indexDir) VAL          : Where to put user and item indexes (optional).
                                 Default: 'id-indexes'
 -o (--output) VAL             : Output directory for recs. There will be two
                                 subdirs one for the primary recommender and
                                 one for the secondry/cross-recommender each of
                                 which will have item similarities and user
                                 history recs.
 -r (--recsPerUser) N          : Number of recommendations to return for each
                                 request. Default = 10.
 -s (--similarityType) VAL     : Similarity measure to use. Default SIMILARITY_L
                                 OGLIKELIHOOD. Note: this is only used for
                                 primary recs and secondary item similarities.
 -t (--tempDir) VAL            : Place for intermediate data. Things left after
                                 the jobs but erased before starting new ones.
 -uidc (--userIDColumn) N      : Which column has the userID (optional).
                                 Default: 0
 -x (--xRecommend)             : Create cross-recommender for multiple actions
                                 (optional). Default: false.
```
## Task Pipeline

The RecommenderUpdateJob runs various subjobs, some of which can be run separately. This job will performs the following tasks:
  1. Ingest text logfiles splitting into DistributedRowMatix(es) one per action
  2. Write out BiMaps that translate External string item and user IDs to and from Internal Mahout long IDs.
  3. Calculate [B'B] with LLR using the Mahout Item-based Recommender job to get the item-item 'similarity matrix' and write these to Solr for indexing.
  4. Calculate [A'B] using Mahout transpose and matrix multiply jobs to get the 'cross-similarity matrix', write these to Solr for indexing
  4.5  Calculates all recommendations and cross-recommendations for all users using the mapreduce version of the Mahout Recommender and XRecommender. For use of Solr these are optional but may be useful to compare to Solr results.
  5. Solr indexes the various fields and is ready to return raw recommendations.
  6. Queries, consisting of user history vectors of itemIDs are fed to Solr. If the primary action is being used for recommendations, the primary action field of the index is queried. If both primary (recommendations) and secondary (cross-recommendations) are desired both fields are queried. If item similarity is required, the doc associated with an item ID is returned indicating similar items. This document field will be ordered by the rank of similarity that any item has with the doc item.
  7. Solr returns a ranked list of items.

## RecommenderUpdateJob

This main task fires off all the subtasks each of which is a separate CLI accessible job. The main sub jobs are:
1. ActionSplitterJob
2. RecommenderJob
2.1 PreparePreferencematrixJob
3. XrecommenderJob
3.2 PrepareActionMatricesJob
4. WriteToSolrJob
4.1 JoinDRMsWriteToSolr -- not a job but a Cascading flow for mapreduce processing of DRMs into CSVs

## RecommenderUpdateJob Output

The job can either pre-calculate all recs and similarities for all users and items OR it can output the similairty matrix to Solr for use as an online recommender. In this later case [B'B] and optionally [B'A] can be written to Solr. Then a user's history, as a string of item IDs, can be used as a query to return recommended items. If a specific item ID's document is fetched it will contain an ordered list of similar items.

The job takes a directory, which is searched recursively for files matching the pattern passed in (only substring match for now). The output will be a preference file per action type, a similairty matrix (two if doing --xrecommend), pre-calculated recs (also cross-recs if doing --xrecommend). The output is structured:

```
output
  |-- item-links-docs [B'B] and [B'A] are joined by item id in these output files. They are HFS part files in CSV text format.
  |                   A header is included on each part that describes the solr fields
  |-- user-history-docs B and A are joined by user id in these output files. They are HFS part files in CSV text format.
  |                   A header is included on each part that describes the solr fields
  |-- actions
  |     |-- p-action DRM containing user history for the primary action
  |     \-- s-action DRM containing user history for the secondary action
  |-- id-indexes
  |     |-- item-index serialized BiHashMap of items for all actions and users, externalIDString <-> internalIDInt
  |     |-- user-index serialized BiHashMap of users for all actions and items, externalIDString <-> internalIDInt
  |     |-- num-items.bin total number of items for all actions
  |     \-- num-users.bin total number of users for all actions
  |-- prefs
  |     |-- 'action1'
  |     |     \-- 'action1.[tsv|csv]' contains preferences from action1 with Mahout IDs = ints. These IDs
  |     |         are Keys into the ID BiMap index. They will return the string used as item or
  |     |         user ID from the original logs files. The file is input to the Mahout RecommenderJob.
  |     |-- 'action2'
  |     |      \-- 'action2.[tsv|csv]' contains preferences from action2 with Mahout IDs = ints. These IDs
  |     |         are Keys into the ID BiMap index. They will return the string used as item or
  |     |         user ID from the original logs files. The file is input to the  XRecommenderJob.
  |     \-- other
  |            \-- other.[tsv|csv] contains preferences from actions other than action1 or action2
  |-- p-recs
  |     |-- recs
  |     |     \-- part-xxxx sequence files containing Key = org.apache.mahout.math.VarLongWritable, Value = org.apache.mahout.cf.taste.hadoop.RecommendedItemsWritable. Key = userID, Value a weighted set of ItemIDs
  |     \-- sims
  |           \-- part-xxxx sequence files making up a DistributedRowMatrix of Key = org.apache.hadoop.io.IntWritable, Value = org.apache.mahout.math.VectorWritable.
  |     |         This is the item similarity matrix, Key = itemID, Value a weighted set of ItemIDs
  |     |         indicating strength of similairty.
  \-- s-recs
        |-- recs
        |     \-- part-xxxx sequence files making up a DistributedRowMatrix of Key = org.apache.hadoop.io.IntWritable, Value = org.apache.mahout.math.VectorWritable
        |         This is the user cross-recommendation matrix, Key = userID, Value a weighted set of ItemIDs
        |         indicating strength of recommendation.
        \-- sims
              \-- part-xxxx sequence files making up a DistributedRowMatrix of Key = org.apache.hadoop.io.IntWritable, Value = org.apache.mahout.math.VectorWritable
                  This is the item cross-similarity matrix, Key = itemID, Value a weighted set of ItemIDs
                  indicating strength of similairty.
```
## Theory

The concept behind this is based on the fact that when preferences are taken from user actions, it is often useful to use one action for recommendation but the other will also work if the secondary action co-occurs with the first. For example views are predictive of purchases if the viewed item was indeed purchased. As long as the user ID is the same for both action A and Action B the items do not have to be the same. In this way, a user's actions on items in B can be correlated to recommend items from B.

This first cut creates and requires unified item ids across both B and A but there is no theoretical or mathematical requirement for this and there are some interesting use cases that use different item IDs. Therefor this can be used in cases where the same user takes two different actions and you want to use them both to recommend the primary action. For example users' purchases can be used to recommend thing a given user might want to purchase and users' views can be used to recommend purchases. Adding the two recommendation lists may well yield better recommendations than either alone.

```
A = matrix of action2 by user, used for cross-action recommendations for example views.
B = matrix of action1 by user, these are the primary recommenders actions for example purchases.
H_a1 = all user history for recommendations of action1 in column vectors. This may be all action1's recorded and so may = B' or it may have truncated history to get more recent activity in recs.
H_a2 = all user history for recommendations of action2 in column vectors. This may be all action2's recorded and so may = A' or it may have truncated history to get more recent activity in recs.
[B'B]H_a1 = R_a1, recommendations from action1. Recommendation are for action1.
[B'A]H_a2 = R_a2, recommendations from action2 where there was also an action1. Cross-recommendations are for action1.
R_a1+ R_a2 = R, assumes a non-weighted linear combination, ideally they are weighted to optimize results.
```

## TBD

Happy path works, creating the two HFS part file directories of text files for indexing by Solr. Many other options are not yet supported or tested.

1. Output to Solr docs is working but indexing not tested. If someone want to try, the fields should be of type 'string' to avoid stop word detection and some other things in Lucene that are not desired in this case.
2. not all options are accepted by the main driver nor are they forwarded to the sub jobs properly. These need to be checked.
3. input log files are of default config in the resources so other formats need to be tested.
4. the only test is to hand run and check by eye using the supplied the bash script, this should be a unit test with output verification.
5. Only 2 actions are working at present. The --xRecommend option is needed with two types of actions.
