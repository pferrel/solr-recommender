solr-recommender
================

Recommender using Solr for recommendation queries and Mahout to generate the similarity matrix model. This implmentation has several unique features:
* Creates models for single actions recommendations and two-action cross-recommendations.
* Both models are available as pre-calculated values (all recommendations for all users) and for online query using potentially  recently generated action data not yet incorporated in the models.
* Ingests text files in CSV or TSV form including arbitrary data along with actions, user, and item ids. Turns these 'log' files into a form compatible with Mahout.
* Uses Mahout to pre-calculate the necessary item similarity matrix and all recommendations for all users.
* Turns the Mahout item similarity matrix into text docs in Solr format--in Solr terms this is one doc (item id) per row and the doc contents in a column (similar items stored as item ids).
* Encodes user history in CSV files with one row per user (user id) and items for a given action in columns (item ids).
* Once indexed by Solr (outside the scope of this project) using a user's action history as a query on the item similarity matrix will yield recommendations as an ordered list of results.

## Usage Scenario

A collaborative filtering type recommender takes
user-id, item-id, preference-weight

the Solr-recommender takes:
user-id (string usually), item-id (likewise a string), action-id (purchase, thumbs-up, like, so a string constant), and any number of other fields, which will be ignored.

It expects CSV or TSV text log type files as input (the delimiter is configurable). It will separate out the actions and use the primary action for recommendations. For instance if you have log data that has purchases, detail-views, add-to-carts, it will separate out the actions into different directories. For this example you would probably want to use only the purchase actions to make recommendations. The solr-recommender will take the raw log files, find the action you ask it to in the column you specify, create index and reverse-index for user and item ids and create the input that Mahout expects, which is:
user-id-key, item-id-key, 1 (for purchase or whatever action is to be recommended). Mahout IDs are ordinal integers so the index/reverse index is kept external to Mahout.

Mahout expects integer values for the id-keys so the solr-recommender creates them but remembers the original string for later. Then the solr-recommender runs mahout to calculate certain intermediate models that are needed to make recommendation. It converts them all back to the original string-type user and item ids and outputs them into directories where Solr can index them.

In the end you will have text delimited files (CSV, TSV or your choice) that have your original ids of the form
item-id, list-of-item-ids
the list-of-item-ids is a single field space delimited (this too can be specified) since Solr automatically uses whitespace delimiters.

*Note:* This is as far as this project takes you--creating the input for Solr indexing and queries.

You will then index the output with Solr.  When you use a user history vector of the same form as  list-of-item-ids for the query against the above index it will return an ordered list of item-ids just like it would if it were searching against doc content and returning doc-ids. The item-ids correspond to recommendations for the user, whose history you used as the query.

The typical use is something like: at runtime you have some user-id that allows you to lookup their history or you have a list-of-item-ids for the 'current user'. You use the history as a query against the item-id, list-of-item-ids to get recs.

You can also use a list-of-item-ids for the item being viewed as a query, in which case you will get "other people who like this item also liked these' type recommendations (item similarity recs).

Solr also allows you to add fields for other metadata like categories or genres. Then mix in this in queries to weight the recs towards the category or genre.

## Getting Started

To run this job you will need to install hadoop 1.X, mahout (>=0.9-SNAPSHOT), and this project. To get recommendations you will also need Solr (>=4.2). Once you have it all setup go to the root of this project and run
```
~$ mvn install
```

This will build the job jar with all dependencies in ./target/solr-recommender-0.1-SNAPSHOT-job.jar. The various tasks inside the jar are described below. Each CLI accessible job comes with a fairly complete description of its parameters printed out by running with no parameters.

There is a sample script for running the job in scripts/solr-recommender. To get a trivial sample output go to the solr-recommender/scripts directory and run:
```
~$ ./solr-recommender
```
This will use simple sample data from the project and create sample output in the project root dir.

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

## How To Generate Recommendations

1. Set up some data store that you plan to use Solr with. This can be HDFS or a Database or even a local filesystem. I generally use a database because some of the features of Solr will allow the seamless blending if content, metadata, and collaborative filtering to make recommendations.
2. Store the contents of the item-link-docs csv file in the place you will use solr to index. For me this is a collection in MaongoDB representing the items in a catalog. Each of the fields in the CSV (only one if you are not doing --xrecommend) will be stored as an attribute of the item. Ideally store them as a string array. In my case I have to store them as a space delimited sting of item tokens.
3. Using Solr 4.2 or greater index the collection of items
4. At runtime when you wish to make recommendations of the simplest form make a fulltext query of the current user's preferred items on the collection of items specifying the query to be on the field where the item-links are kept (the place previously indexed by Solr).
5. The results of this query will be an ordered list of recommended items.
6. To bias the query towards a category or other metadata stored with the items simple index that field along with the item-links. Then in the query apply the user history to the item-links field and the category to the categories field of your item collection. This will return recommended items biased towards ones that include the category supplied in the query. Boost the category and get more bias.
7. For users who have no history you can make item similarity recommendations in certain cases. Imagine the user is looking at an item. You may want to form a query using the item-links for the one being viewed and bias results by the categories of the item viewed. This can be done with no user history. You could describe this recommendation set as "people who liked this item also liked these, in similar categories"

As you can see the use is extremely flexible and since the flexibility is in how the query is formed, it makes the system very easy to experiment with.

## How To Retrain The Recommender

Recommendations can be made with initial data for some time but as new users express their preferences and as new items are added to the collection you will want to re-train the recommender. This is done by recalculating the item-links on all data. Add your new preferences to the total data and re-run the RecommenderUpdateJob on it. Re-import the item-links to your collection and have Solr reindex. If you are using a DB to store the collection and item-links the reindex may be done automatically.

One thing to note about any recommender is that no item without preferences can be recommended. So as new items are added to you catalog collection you'll want to retrain the recommender. Also more preference data usually improves recommendation so retrain as new preferences are available.

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

 2. The creation of the indexes is a single machine single threaded operation. It creates an in-memory BiHashMap (Guava collections) and eventually writes it to a delimited file. This BiHashMap is instantiated on every cluster machine once in memory to do External <-> Mahout id lookups. All hadoop jobs on the node reference this single in memory index.

##Known Problems
1.  To be safe, use the full path to the input preferences.