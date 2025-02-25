## Problem 1 - Recent Posts 
**<ins>Objectives:</ins>**
- Show off the most 10 recent posts that has a fast enough update times in order to be displayed on a screen at a building lobby.

**<ins>Changes made:</ins>**

Since the provided query is dependent on timestamp for retrieving the 10 most
recent posts, index should be created on the timestamp in descending order. 
With this, the data base can utilize a B+ tree data structure to quickly fetch information
 that is stored in descending order:

`CREATE INDEX post_timestamp_idx ON posts(post_timestamp DESC);`

Instead of doing a table scan through a million of rows to sort, the query can 
now instead just fetch the 10 most recent posts immediately. 

After the creation of index based on post timestamp, run time of the query 
has reduced from 4841 ms to 8 ms!

|        | PUBLIC | scanCount | Query time      |
|--------|---|---|-----------------|
| Before | PUBLIC.POSTS.tableScan | 995087 | 10 rows, 4841 ms |
 | After | PUBLIC.POST_TIMESTAMP_IDX | 10 | 10 rows, 8 ms    |


**<ins>EXPLAIN ANALYZE screenshot:</ins>**

| Before | After |
|--- | --- |
|<img src="https://github.com/eburhansjah/ec500-spring2025-eburhansjah-h2database/blob/hw4-eburhansjah-h2database/assets/before-hw4-prob1.png" alt="before-explain-analyze-img-hw4-prob1" style="width:50%; height:auto;">|<img src="https://github.com/eburhansjah/ec500-spring2025-eburhansjah-h2database/blob/hw4-eburhansjah-h2database/assets/explain-analyze-hw4-prob1.png" alt="after-explain-analyze-img-hw4-prob1" style="width:80%; height:auto;">|

 
## Problem 2 - Somewhat Strange Query
**<ins>Objectives:</ins>**
- Reduce query time from 300-600 ms to below 100 ms

**<ins>Changes made:</ins>**
- Creating composite index: `CREATE INDEX posts_composite_idx ON posts (post_timestamp ASC, content, author);`

With the creation of composite index, the query run time was reduced to below 100 ms. Infact, it was at around 10 - 20 ms!

Composite index optimizes the query that filters based on content, post_timestamp and author. The composite index was created with columns with higher selectivity ahead of those with lower selectivity. 

|        | PUBLIC | scanCount | Query time      |
|--------|---|---|-----------------|
| Before | PUBLIC.POSTS.tableScan | 995087 | 46 rows, 636 ms |
| After | PUBLIC.POSTS_COMPOSITE_IDX | 16874 | 46 rows, 13 ms    |
 
**<ins>EXPLAIN ANALYZE screenshot:</ins>**

| Before | After |
|--- | --- |
|<img src="https://github.com/eburhansjah/ec500-spring2025-eburhansjah-h2database/blob/hw4-eburhansjah-h2database/assets/explain-analysis-hw4-prob2-before.png" alt="before-explain-analyze-hw4-prob2" style="width:100%; height:auto;"> | <img src="https://github.com/eburhansjah/ec500-spring2025-eburhansjah-h2database/blob/hw4-eburhansjah-h2database/assets/explain-analysis-hw4-prob2-after.png" alt="after-explain-analyze-hw4-prob2" style="width:100%; height:auto;">|


Intially, I experimented with the following:

**Attempt 1:** Creating indexes separately for timestamp in ascending order, content, and author

Result from attempt 1 did not reduce query time to below 100 ms. In fact, it was in ~200 - 300 ms

**Attempt 2:**

- Creating indexes separately for timestamp in ascending order, content, and author
- Replacing commands UPPER() and SUBSTR() to content LIKE '%C' and to author LIKE '__son%' respectively

Result from attempt 2 also did not reduce query time to below 100 ms. In fact, it was also at around ~200 - 300 ms
 
## Problem 3 - Really Fast Single Row Responses
### Problem 3.1 
 
**<ins>a) Analyze Explain Analyze outpout for the query when no indexes exist</ins>**

<img src="https://github.com/eburhansjah/ec500-spring2025-eburhansjah-h2database/blob/hw4-eburhansjah-h2database/assets/explain-analyze-hw4-prob3-1-without-indexes.png" alt="explain-analyze-hw4-prob3-1-without-indexes" style="width:40%; height:autho;">

The screenshot above is the output of Explain Analyze for the query when no indexes exist. We can observe that the data base is doing a table scan that involves 995087 total number of scans, which gives the following query time: (3 rows, 589 ms).

**<ins>b) Create different indexes on the same columns and analyze outcomes</ins>**

In class, there are two different physical index strategies being discussed. They are B+ tree indexes and Hash indexes. B+ tree indexes are for commonly used for general purposes and when it comes to exact match queries, run time is O(log N). Meanwhile, Hash indexes are more specialized for exact match queries, which gives a constant run time of O(1).

I created four types of indexes:
- `CREATE INDEX post_timestamp_btree_idx on posts(post_timestamp);`
- `CREATE INDEX post_timestamp_btree_desc_idx on posts(post_timestamp DESC);`
- `CREATE HASH INDEX post_timestamp_hash_idx on posts(post_timestamp);`
- `CREATE HASH INDEX post_timestamp_hash_desc_idx on posts(post_timestamp DESC);`

The following are the observed results from EXPLAIN ANALYZE:

| Index type | PUBLIC | scanCount | Query time      |
|--------|---|---|-----------------|
| Without index | PUBLIC.POSTS.tableScan | 995087 | 1 row, 427 ms |
| B+ tree (ASC) | PUBLIC.POST_TIMESTAMP_BTREE_IDX | 4 | 1 row, 18 ms |
| B+ tree (DESC) | PUBLIC.POST_TIMESTAMP_BTREE_DESC_IDX | 4 | 1 row, 38 ms |
| Hash (ASC) | PUBLIC.POST_TIMESTAMP_HASH_IDX | 4 | 1 row, 23 ms |
| **Hash (DESC)** | PUBLIC.POST_TIMESTAMP_HASH_DESC_IDX | 4 | **1 row, 14 ms** |

From the table above, hash index in descending order on timestamp gives the fastest query time. This makes sense because not only is hash index is specialized for exact match queries. Theoretically ASC or DESC ordering does not matter for hash data structure, but there is an observable effect on query time albeit a small difference.

<ins>EXPLAIN ANALYZE SCREENSHOTS:</ins>
- B+ tree (ASC) index on post_timestamp
   - <img src="https://github.com/eburhansjah/ec500-spring2025-eburhansjah-h2database/blob/hw4-eburhansjah-h2database/assets/hw4-prob3-1-btree-asc-idx.png" alt="btree-asc-timestamp" style="width:60%; height:auto;">

- B+ tree (DESC) index on post_timestamp
  - <img src="https://github.com/eburhansjah/ec500-spring2025-eburhansjah-h2database/blob/hw4-eburhansjah-h2database/assets/hw4-prob3-1-btree-desc-idx.png" alt="btree-desc-timestamp" style="width:60%; height:auto;">
  
- Hash (ASC) index on post_timestamp
  - <img src="https://github.com/eburhansjah/ec500-spring2025-eburhansjah-h2database/blob/hw4-eburhansjah-h2database/assets/hw4-prob3-1-hash-asc-idx.png" alt="hash-asc-timestamp" style="width:60%; height:auto;">
  
- Hash (DESC) index on post_timestamp
  - <img src="https://github.com/eburhansjah/ec500-spring2025-eburhansjah-h2database/blob/hw4-eburhansjah-h2database/assets/hw4-prob3-1-hash-desc-idx.png" alt="hash-desc-timestamp" style="width:60%; height:auto;">

**<ins>c) What index does H2DB end up using?  Explain the pros and cons of each index that you created.</ins>**

After creating all four indexes, H2DB ends up using the B+ tree (DESC) index.

Pros:
- Overall, the four types of indexes significantly reduce query run time from ~400 ms to ~10-40 ms for a row
- Having either indexes in DESC order is that it allows the query to quickly fetch data from the more recent dates
- Hash indexes are geared to exact match queries, giving a run time of O(1)

Cons:
- If the query is looking to fetch information from earlier date, the DESC order of either of the indexes would result in slower query time. ASC order would be used instead
- B+ tree has a more general purpose in query. Because of tree traversal, exact match query will not be done at constant time and instead in O(log N)
- Hash indexes can be problematic if there are many collisions

 
### Problem 3.2 
﻿
**<ins>Which of the indexes that you created for 3.1 would you expect to be used now.  Please explain.</ins>**

```
SELECT
    post_id,
    post_timestamp
 
FROM
    posts
 
WHERE
    post_timestamp BETWEEN '2024-01-25' AND '2024-01-27';
```

For the query above, since the WHERE clause is filtering based on a range, Hash indexes cannot be used. Hence, the indexes that would be applicable is B+ tree indexes, either in ASC or DESC order. 
 
### Problem 3.3

```
SELECT
    post_id,
    post_timestamp,
    content
 
FROM
    posts
 
WHERE
    post_timestamp BETWEEN '2024-01-25' AND '2024-01-27';
```

**<ins>a) Look at Explain Analyze output for query with indexes in place from 3.2:</ins>**

| Index type | PUBLIC | scanCount | Reads | Query time      |
|--------|---|---|-----------------| --- |
| Without index | PUBLIC.POSTS.tableScan | 293977 | 19148 | 1 row, 198 ms |
| B+ tree (ASC) | PUBLIC.POST_TIMESTAMP_BTREE_IDX | 1000 | 1963 | 1 row, 212 ms |
| B+ tree (DESC) | PUBLIC.POST_TIMESTAMP_BTREE_DESC_IDX | 1000 | 1975 | 1 row, 62 ms |

<ins>EXPLAIN ANALYZE SCREENSHOTS:</ins>
- Without index
  - <img src="https://github.com/eburhansjah/ec500-spring2025-eburhansjah-h2database/blob/hw4-eburhansjah-h2database/assets/hw4-prob3-3-without-idx.png" alt="3-3-without-index" style="width:50%; height:auto;">
  
- B+ tree (ASC) index on post_timestamp
  - <img src="https://github.com/eburhansjah/ec500-spring2025-eburhansjah-h2database/blob/hw4-eburhansjah-h2database/assets/hw4-prob3-3-btree-asc.png" alt="3-3-btree-asc-timestamp" style="width:50%; height:auto;">

- B+ tree (DESC) index on post_timestamp
  - <img src="https://github.com/eburhansjah/ec500-spring2025-eburhansjah-h2database/blob/hw4-eburhansjah-h2database/assets/hw4-prob3-3-btree-desc.png" alt="3-3-btree-desc-timestamp" style="width:50%; height:auto;">


**<ins>b) Modify one of the indexes in 3.2 to make query go faster</ins>**

Changes made from one of the indexes in 3.2 would be to add another index column, content:

`CREATE INDEX post_desc_timestamp_content_btree_idx on posts(post_timestamp DESC, content);`

The following is Explain Analyze output for modified query:

| Index type | PUBLIC | scanCount | Reads | Query time      |
|--------|---|---|-----------------| --- |
| B+ tree | PUBLIC.POST_DESC_TIMESTAMP_CONTENT_BTREE_IDX | 1000 | 48 | 1 row, 68 ms |

<ins>EXPLAIN ANALYZE SCREENSHOT:</ins>

<img src="https://github.com/eburhansjah/ec500-spring2025-eburhansjah-h2database/blob/hw4-eburhansjah-h2database/assets/hw4-prob3-3-mod-idx.png" alt="3-3-mod-idx" style="width:50%; height:auto;">

**<ins>c) Explain why it makes query go faster</ins>**

Having composite index of timestamp(DESC), content decreases query time because covering indexes prevents the data base from getting content values from the core table via the primary key. In other words, all of the required columns from SELECT (post_id, post_timestamp, and content) are stored in the index itself, thus the database do not need to spend extra time to hop around pages/core table to retrieve information on content, resulting to a faster query.

 
## Problem 4 - Table Join Order

```
SELECT
   COUNT(1)
 
FROM
    posts
    JOIN followers ON posts.author = followers.following_handle
    JOIN users ON followers.follower_handle = users.handle
 
WHERE
    users.last_name = 'Anderson'
    AND users.first_name = 'Abigail';
```

### Problem 4.1 

**Can you make this query faster by only re-arranging the JOIN order of the tables in the FROM clause and without making any other changes?** 

Yes, I can make this query faster. The current query that we have above is inefficient because filtering begins with posts table which does not narrow down rows to fit according to the WHERE clause. As a result, the query takes a very long time to run at (1 row, 104341 ms).

My modified query:

```
SELECT
   COUNT(1)

FROM users
    JOIN followers ON users.handle = followers.follower_handle
    JOIN posts ON followers.following_handle = posts.author

WHERE
    users.last_name = 'Anderson'
    AND users.first_name = 'Abigail';
```

The modified query begins filtering from users -> followers -> posts table which was efficient and significantly reduces query run time to (1 row, 24 ms).
 
### Problem 4.2

Three tables are involved in the query above.  Theoretically, that would give us 6 potential permutations on Join order.  However, in practice only 4 are feasible because users and posts cannot be joined to each other directly and followers must appear before at least one of those tables (permutations 5 and 6).  

**List each of the four possible join orders and explain why or why not that particular join order will perform well or poorly.**

| Permutation | Join order | Run time | scanCount | reads |
| ----------- | ---------- | -------- | --------- | ----- |
| 1 | posts -> followers -> users | (1 row, 104341 ms) | 296895399 | 2056458 |
| 2 | users -> followers -> posts | (1 row, 24 ms) | 10217 | 719 |
| 3 | followers -> users -> posts | (1 row, 884 ms) | 2985331 | 44616 |
| 4 | followers -> posts -> users | (1 row, 58218 ms) | 296895307 | 4749762 |
| 5 | users -> posts -> followers | N/A | N/A | N/A | 
| 6 | posts -> users -> followers | N/A | N/A | N/A | 


<ins>EXPLAIN ANALYZE SCREENSHOTS:</ins>
- Permutation 1: posts -> followers -> users
  - The most inefficient query
  - This is due to the massive size of posts table. The join order of this permutation does not help reduce rows effectively by starting with posts table. By leaving the filtering conditions to the end (users table), the data base is forced to process all unnecessary information during the query process.
  - <img src="https://github.com/eburhansjah/ec500-spring2025-eburhansjah-h2database/blob/hw4-eburhansjah-h2database/assets/hw4-4-og-query.png" alt="p-f-u-join-order" style="width:50%; height:auto;">
  
- Permutation 2: users -> followers -> posts
  - The most efficient query
  - This is because we are doing filtering early on users table which is based on the WHERE clause. This significantly reduces the number of rows early, preventing database from unnecessary processing overhead.
  - <img src="https://github.com/eburhansjah/ec500-spring2025-eburhansjah-h2database/blob/hw4-eburhansjah-h2database/assets/hw4-4-1-rearrange-u-f-p-join-order.png" alt="u-f-p-join-order" style="width:50%; height:auto;">
  
- Permutation 3: followers -> users -> posts
  - Inefficient query, but is faster than permutation 1
  - This is because filtering is happing after joining the followers table to the users table (happing at the middle of the join order)
  - <img src="https://github.com/eburhansjah/ec500-spring2025-eburhansjah-h2database/blob/hw4-eburhansjah-h2database/assets/hw4-4-2-f-u-p-join-order.png" alt="f-u-p-join-order" style="width:50%; height:auto;">
  
- Permutation 4: followers -> posts -> users
  - Inefficient query. Faster than permutation 1 and slower than permutation 3
  - This is because although we start with followers table, which is a smaller table compared to the posts table, we are doing the necessary filtering at the end (users table)
  - <img src="https://github.com/eburhansjah/ec500-spring2025-eburhansjah-h2database/blob/hw4-eburhansjah-h2database/assets/hw4-4-2-f-p-u-join-order.png" alt="f-p-u-join-order" style="width:50%; height:auto;">
 
## Problem 5 - Putting it All Together - Fast Most Recent Posts 
 
<your query here>





[![CI](h2/src/docsrc/images/h2-logo-2.png)](https://github.com/h2database/h2database/actions?query=workflow%3ACI)
# Welcome to H2, the Java SQL database.

## The main features of H2 are:

* Very fast, open source, JDBC API
* Embedded and server modes; disk-based or in-memory databases
* Transaction support, multi-version concurrency
* Browser based Console application
* Encrypted databases
* Fulltext search
* Pure Java with small footprint: around 2.5 MB jar file size
* ODBC driver

More information: https://h2database.com

## Downloads

[Download latest version](https://h2database.com/html/download.html) or add to `pom.xml`:

```XML
<dependency>
    <groupId>com.h2database</groupId>
    <artifactId>h2</artifactId>
    <version>2.3.232</version>
</dependency>
```

## Documentation

* [Tutorial](https://h2database.com/html/tutorial.html)
* [SQL commands](https://h2database.com/html/commands.html)
* [Functions](https://h2database.com/html/functions.html), [aggregate functions](https://h2database.com/html/functions-aggregate.html), [window functions](https://h2database.com/html/functions-window.html)
* [Data types](https://h2database.com/html/datatypes.html)

## Support

* [Issue tracker](https://github.com/h2database/h2database/issues) for bug reports and feature requests
* [Mailing list / forum](https://groups.google.com/g/h2-database) for questions about H2
* ['h2' tag on Stack Overflow](https://stackoverflow.com/questions/tagged/h2) for other questions (Hibernate with H2 etc.)
