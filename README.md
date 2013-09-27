solr-recommender
================

Recommender using Solr for recommendation queries and Mahout to generate training data. This implmentation has several unique features:
* Creates models for single actions recommendations and two-action cross-recommendations.
* Both models are available as pre-calculated values (all recommendations for all users) and for online query using potentially  recently generated action data not yet incorporated in the models.
* Ingests text files in CSV or TSV form including arbitrary data along with actions, user, and item ids. Turns these 'log' files into a form compatible with Mahout.
* Uses Mahout to pre-calculate the necessary item similarity matrix and all recommendations for all users.
* Turns the Mahout item similarity matrix into text docs in Solr format--in Solr terms this is one doc (item id) per row and the doc contents in a column (similar items stored as item ids).
* Encodes user history in CSV files with one row per user (user id) and items for a given action in columns (item ids).
* Once indexed by Solr (outside the scope of this project) using a user's action history as a query on the item similarity matrix will yield recommendations as an ordered list of results.

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
  1. Ingest text "log" files splitting into DistributedRowMatix(es) one per action
  2. Write out BiMaps that translate External string item and user IDs to and from internal Mahout long IDs.
  3. Calculate [B'B] with LLR using the Mahout Item-based Recommender job to get the item-item 'similarity matrix' and write these to Solr for indexing.
  4. Calculate [A'B] using Mahout transpose and matrix multiply jobs to get the 'cross-similarity matrix', write these to Solr for indexing
  4.5  Calculates all recommendations and cross-recommendations for all users using the mapreduce version of the Mahout Recommender and XRecommender. These are not neccessary when using Solr only to return recommendations. It should be noted that is is likely the precalculated recommendations and cross-recommendations and the ones returned by Solr will be different. The difference has not be quantified.
  5. Not implemented here: Solr indexes the various fields and is ready to return raw recommendations.
  6. Not implemented here: Queries, consisting of user history vectors of itemIDs are fed to Solr. If the primary action is being used for recommendations, the primary action field of the index is queried. If both primary (recommendations) and secondary (cross-recommendations) are desired both fields are queried. If item similarity is required, the doc associated with an item ID is returned indicating similar items. This document field will be ordered by the rank of similarity that any item has with the doc item.
  7. Not implemented here: Solr returns a ranked list of items.

## RecommenderUpdateJob

This main task fires off all the subtasks each of which is a separate CLI accessible job. The main sub jobs are:
  1. ActionSplitterJob
  2. RecommenderJob
    * PreparePreferencematrixJob
  3. XrecommenderJob
    * PrepareActionMatricesJob
  4. WriteToSolrJob
    * JoinDRMsWriteToSolr -- not a job but a Cascading flow for mapreduce processing of DRMs into CSVs

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
  |               This is the item similarity matrix, Key = itemID, Value a weighted set of ItemIDs
  |               indicating strength of similairty.
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

The concept behind this is based on the fact that when preferences are taken from user actions, it is often useful to use one action for recommendation but the other will also work if the secondary action co-occurs with the first. For example views are predictive of purchases if the viewed item was indeed purchased. As long as the user ID is the same for both action A and Action B the items do not have to be the same. In this way, a user's actions on items in A and B can be correlated to recommend items from B.

This first cut creates and requires unified item ids across both B and A but there is no theoretical or mathematical requirement for this and there are some interesting use cases that use different item IDs. Therefor this can be used in cases where the same user takes two different actions and you want to use them both to recommend the primary action. For example a users' purchases can be used to recommend purchases and a users' views can be used to recommend purchases. Adding the two recommendation lists may well yield better recommendations than either alone.

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

1. Output to Solr docs is working but indexing not tested. If someone wants to try, the fields should be of type 'string' to avoid stop word detection and some other things in Lucene that are not desired in this case.
2. Not all options are accepted by the main driver nor are they forwarded to the sub jobs properly. These need to be checked.
3. The only unit test is to hand run and check by eye using the supplied the bash script, this will be made a unit test with automatic output verification.

##Notes
1. 1 and 2 actions are working. The --xRecommend option is needed with two types of actions. To get only RecommenderJob type output to Solr execute the following from solr-recommender/scripts:
```
~$ hadoop jar ../target/solr-recommender-0.1-SNAPSHOT-job.jar \
       finderbots.recommenders.hadoop.RecommenderUpdateJob \
       --input ../src/test/resources/logged-preferences/ \
       --inputFilePattern ".tsv" \
       --output ../out \
       --tempDir ../tmp
```
This assumes default values for action names etc. See the complete option list. To get cross-recommendations perform the following from solr-recommender/scripts:
```
~$ hadoop jar ../target/solr-recommender-0.1-SNAPSHOT-job.jar \
       finderbots.recommenders.hadoop.RecommenderUpdateJob \
       --input ../src/test/resources/logged-preferences/ \
       --inputFilePattern ".tsv" \
       --output ../out \
       --tempDir ../tmp \
       --xRecommend
```