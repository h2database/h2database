## Problem 1 - Recent Posts 
**<ins>Objectives:</ins>**
- Show off the most 10 recent posts that has a fast enough update times in order to be displayed on a screen at a building lobby.

**<ins>Changes made:</ins>**

Since the provided query is dependent on timestamp for retrieving the 10 most
recent posts, index should be created on the timestamp in descending order. 
With this, the data base can utilize a B-tree data structure to quickly fetch information
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
| Before | PUBLIC.POSTS.tableScan | 995087 | 46 rows, 590 ms |
| After | PUBLIC.POSTS_COMPOSITE_IDX | 16874 | 46 rows, 13 ms    |

Intially, I experimented with the following:

**Attempt 1:** Creating indexes separately for timestamp in ascending order, content, and author

Result from attempt 1 did not reduce query time to below 100 ms. In fact, it was in ~200 ms

**Attempt 2:**

- Creating indexes separately for timestamp in ascending order, content, and author
- Replacing commands UPPER() and SUBSTR() to content LIKE '%C' and to author LIKE '__son%' respectively

Result from attempt 2 also did not reduce query time to below 100 ms. In fact, it was also at around ~200 ms
 
**<ins>EXPLAIN ANALYZE screenshot:</ins>**

| Before | After |
|--- | --- |
 
## Problem 3 - Really Fast Single Row Responses
### Problem 3.1 
 
<What index does H2DB end up using?  Explain the pros and cons of each index that you created.>
 
### Problem 3.2 
﻿
<Which of the indexes that you created for 3.1 would you expect to be used now.  Please explain.>
 
### Problem 3.3
 
<Can you modify one of the indexes from 3.2 to make this query even faster?  Explain why your change to the index made the query even faster.>
 
## Problem 4 - Table Join Order
### Problem 4.1 
 
<Your modified query here>
 
### Problem 4.2
 
<List each of the four possible join orders and explain why or why not that particular join order will perform well or poorly.>
 
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
