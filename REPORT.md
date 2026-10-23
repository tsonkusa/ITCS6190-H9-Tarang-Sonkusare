# Hands-on L9: Report

**Name:** Tarang Sonkusare
**Student ID:** 801352372
**Email:** tsonkusa@charlotte.edu

---

## Seed and commands

**Seed used for `datagen.py`:** `801352372`

I used my student ID as the seed to generate the dataset. The generator created 50 users and 5,000 posts. I completed the lab in GitHub Codespaces using Docker and Apache Spark 4.2.0.

I used the Codespaces Docker Compose configuration instead of the default one because I was running everything through GitHub Codespaces.

The commands I used were:

```bash
# Generate the data
python3 datagen.py 801352372

# Verify the input files
ls -lh shared-folder/input/
wc -l shared-folder/input/*.csv

# Start the Spark cluster
docker compose -f docker-compose.codespaces.yml up -d

# Check the containers
docker ps
docker compose -f docker-compose.codespaces.yml ps

# Copy main.py into the Spark master
docker cp main.py spark-master:/opt/spark/work-dir/

# Run the analysis
docker exec -i spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/main.py \
  /opt/spark/work-dir/shared/input \
  /opt/spark/work-dir/shared/output \
  > final_run.log 2>&1

# View the generated results
cat shared-folder/output/task1/part-*.csv
cat shared-folder/output/task2/part-*.csv
cat shared-folder/output/task3/part-*.csv
cat shared-folder/output/task4/part-*.csv
```

I implemented and tested the four tasks one at a time. After completing each task, I copied the updated `main.py` into the Spark master container and reran the analysis.

The cluster successfully connected to two Spark workers. The program loaded all 5,000 posts and 50 users and generated the required output files.

---

## Results

### Task 1: Hashtag trends (DataFrame API)

**Output:**

```text
hashtag,count
#fail,1742
#bug,1697
#darkmode,1190
#design,1171
#mood,739
#social,737
#love,731
#update,720
#ux,495
#tech,263
```

For this task, I used the DataFrame API to split the comma-separated hashtags, explode them into individual rows, and normalize the hashtags using `lower()` and `trim()`. I then grouped and counted the hashtags and selected the top 10.

The most frequently used hashtag was `#fail`, with 1,742 occurrences, followed by `#bug`, with 1,697 occurrences. Both appeared much more frequently than the other hashtags, suggesting that complaints or problems were common topics in the generated posts.

**How many distinct spellings did your most frequent hashtag have before `lower` and `trim`?**

The most frequent hashtag, `#fail`, appeared in **2 distinct spellings**:

- `#fail`: 1,485 occurrences
- `#FAIL`: 257 occurrences

After converting both spellings to lowercase, the combined count was 1,742.

This demonstrates why normalizing text before grouping is important. Without normalization, the same hashtag would be counted as two different values.

### Task 2: Engagement by age group (Spark SQL)

**Output:**

```text
age_group,posts,avg_likes,avg_retweets
Adult,2225,54.74,13.92
Teen,867,54.67,13.86
Senior,1908,50.1,11.93
```

I used a Spark SQL query to join the `posts` and `users` temporary views on `user_id`. I then grouped the results by age group and calculated the number of posts, average likes, and average retweets.

Adults had the highest average engagement, with 54.74 likes and 13.92 retweets per post. Teens were very close, with 54.67 likes and 13.86 retweets. Seniors had the lowest averages, with 50.10 likes and 11.93 retweets.

Adults also contributed the most posts, with 2,225 out of 5,000. All three age groups together accounted for the complete dataset.

I also implemented the same query using the DataFrame API to compare how Spark executes equivalent operations written in two different languages.

### Task 3: Sentiment versus engagement (Spark SQL)

**Output:**

```text
sentiment,posts,avg_likes,avg_retweets
Negative,1043,24.97,6.78
Neutral,2254,48.3,11.82
Positive,1703,76.26,18.81
```

For this task, I used a SQL `CASE` expression to classify each post by sentiment score:

- Positive: score greater than 0.3
- Negative: score less than -0.3
- Neutral: all remaining scores

I then calculated the total number of posts and average engagement for each category.

Neutral posts were the most common, with 2,254 posts, followed by positive posts with 1,703 and negative posts with 1,043.

**Does sentiment pay on your data?**

Yes, positive sentiment was associated with significantly higher engagement in my dataset.

Positive posts averaged 76.26 likes and 18.81 retweets, compared with 24.97 likes and 6.78 retweets for negative posts.

That means positive posts received approximately **3.05 times as many likes** and **2.77 times as many retweets** as negative posts.

Neutral posts fell between the two categories, averaging 48.30 likes and 11.82 retweets.

These results show a strong association between sentiment and engagement in the generated dataset, although they do not establish that positive sentiment directly causes higher engagement.

### Task 4: Top verified users (DataFrame API)

**Output:**

```text
username,total_likes,total_retweets,reach
@pixel_pusher39,20668,5768,26436
@techie50,21127,4843,25970
@night_coder36,10103,2503,12606
@pixel_pusher4,8924,2272,11196
@stream_bot26,7749,1735,9484
```

I joined the posts with the users DataFrame, filtered for verified users, and grouped the data by username.

For each verified user, I calculated total likes, total retweets, and reach using:

`reach = total_likes + total_retweets`

I then sorted the users by reach in descending order and selected the top five.

The user with the highest reach was `@pixel_pusher39`, with 26,436, followed by `@techie50`, with 25,970.

An interesting finding was that `@techie50` received more total likes (21,127) than `@pixel_pusher39` (20,668). However, `@pixel_pusher39` received more retweets, resulting in the highest overall reach.

This shows that both likes and retweets affect the combined reach metric.

---

---

## Two languages, one plan

I implemented Task 2 using Spark SQL and the DataFrame API. Both
implementations produce the same result.

I also verified this using `exceptAll()` in both directions, and the
program printed:

`TASK 2 SQL AND DATAFRAME RESULTS MATCH`

### Spark SQL physical plan

```text
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Sort [avg_likes#54 DESC NULLS LAST], true, 0
   +- Exchange rangepartitioning(avg_likes#54 DESC NULLS LAST, 200), ENSURE_REQUIREMENTS, [plan_id=697]
      +- HashAggregate(keys=[age_group#10], functions=[count(1), avg(likes#4), avg(retweets#5)])
         +- Exchange hashpartitioning(age_group#10, 200), ENSURE_REQUIREMENTS, [plan_id=694]
            +- HashAggregate(keys=[age_group#10], functions=[partial_count(1), partial_avg(likes#4), partial_avg(retweets#5)])
               +- Project [likes#4, retweets#5, age_group#10]
                  +- BroadcastHashJoin [user_id#1], [user_id#8], Inner, BuildRight, false, false
                     :- Filter isnotnull(user_id#1)
                     :  +- FileScan csv [user_id#1,likes#4,retweets#5] Batched: false, DataFilters: [isnotnull(user_id#1)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/posts.csv], PartitionFilters: [], PushedFilters: [IsNotNull(user_id)], ReadSchema: struct<user_id:string,likes:int,retweets:int>
                     +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, false]),false), [plan_id=689]
                        +- Filter isnotnull(user_id#8)
                           +- FileScan csv [user_id#8,age_group#10] Batched: false, DataFilters: [isnotnull(user_id#8)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/users.csv], PartitionFilters: [], PushedFilters: [IsNotNull(user_id)], ReadSchema: struct<user_id:string,age_group:string>
```

### DataFrame API physical plan

```text
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Sort [avg_likes#134 DESC NULLS LAST], true, 0
   +- Exchange rangepartitioning(avg_likes#134 DESC NULLS LAST, 200), ENSURE_REQUIREMENTS, [plan_id=747]
      +- HashAggregate(keys=[age_group#10], functions=[count(1), avg(likes#4), avg(retweets#5)])
         +- Exchange hashpartitioning(age_group#10, 200), ENSURE_REQUIREMENTS, [plan_id=744]
            +- HashAggregate(keys=[age_group#10], functions=[partial_count(1), partial_avg(likes#4), partial_avg(retweets#5)])
               +- Project [likes#4, retweets#5, age_group#10]
                  +- BroadcastHashJoin [user_id#1], [user_id#8], Inner, BuildRight, false, false
                     :- Filter isnotnull(user_id#1)
                     :  +- FileScan csv [user_id#1,likes#4,retweets#5] Batched: false, DataFilters: [isnotnull(user_id#1)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/posts.csv], PartitionFilters: [], PushedFilters: [IsNotNull(user_id)], ReadSchema: struct<user_id:string,likes:int,retweets:int>
                     +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, false]),false), [plan_id=739]
                        +- Filter isnotnull(user_id#8)
                           +- FileScan csv [user_id#8,age_group#10] Batched: false, DataFilters: [isnotnull(user_id#8)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/users.csv], PartitionFilters: [], PushedFilters: [IsNotNull(user_id)], ReadSchema: struct<user_id:string,age_group:string>
```

### Comparison

The two plans have the same operator structure, although internal
expression IDs and plan IDs differ.

Both plans begin with two CSV file scans. Spark uses an inner
`BroadcastHashJoin` with `BuildRight`, broadcasting the smaller
users dataset rather than shuffling both datasets for the join.

Both plans use a partial `HashAggregate`, followed by an
`Exchange hashpartitioning(age_group, 200)` and a final
`HashAggregate`. This shuffle brings matching age groups
together for the final aggregation.

The `BroadcastExchange` distributes the small users table to
the executors. The `Exchange rangepartitioning(avg_likes DESC, 200)`
supports the final global sorting by average likes.

The `Sort` operator orders the output by average likes in
descending order.

Both plans show `AdaptiveSparkPlan isFinalPlan=false`, meaning
these are the plans displayed by `explain()` before the final
adaptive execution plan is available.

This demonstrates that Spark SQL and the DataFrame API can
produce equivalent execution strategies. Both use Spark's
optimizer, so the choice between them can be based on readability
and the requirements of the application.

---

## Views

`createOrReplaceTempView("posts")` registered the posts DataFrame
as a temporary SQL view named `posts`. The same method registered
the users DataFrame as `users`.

This allowed my Spark SQL queries to reference the datasets by
name.

Creating these views did not copy, materialize, or duplicate
the underlying data. The views reference the DataFrame logical
plans.

Temporary views exist only within the Spark session. Once the
session ends, the views disappear.

To keep a dataset available for another session, I could write
it to persistent storage such as Parquet or save it as a table
using `saveAsTable()` with a persistent catalog and storage.

---

## Problems and fixes

I did not encounter any fatal errors while completing the four
tasks.

The initial program displayed messages such as:

`=== task1: not implemented yet ===`

This was expected because the starter functions returned `None`.
I implemented the functions one at a time and reran the program.

I used `docker-compose.codespaces.yml` because the assignment
provides that configuration specifically for GitHub Codespaces.
All three containers started successfully, and two workers
registered with the Spark master.

Spark printed this warning:

`WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable`

This was nonfatal and did not require a fix.

I redirected the execution output to log files so I could
inspect the physical plans and check for runtime errors.

The final run reported:

`INFO SparkContext: Successfully stopped SparkContext`

All four tasks generated output CSV files successfully.

---

## Conclusion

This lab demonstrated how to analyze social media data using
both Spark SQL and the DataFrame API.

I practiced joining datasets, grouping, aggregating, exploding
hashtags, normalizing text, calculating reach, and using SQL
CASE expressions.

The physical-plan comparison showed that Spark can produce
equivalent execution strategies for SQL and DataFrame queries.

