---
title: "A Deep Dive Into collectNumberOrderedElements"
author: Eyal Allweil and Ben Rahamim
license: >
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
---
# A Deep Dive Into collectNumberOrderedElements

by Eyal Allweil and Ben Rahamim

![](https://plus.unsplash.com/premium_photo-1681586533774-1d9d42425712?q=80&w=800&auto=format&fit=crop)

Photo from [Unsplash](https://unsplash.com/photos/a-black-and-white-photo-of-a-network-of-lines-qCodK5vqaNs)

---

**_As with many Apache projects with robust communities and growing ecosystems,_** [**_Apache DataFu_**](http://datafu.apache.org/) **_has contributions from individual code committers employed by various organizations. Users of Apache projects who contribute code back to the project benefits everyone. This is part three of PayPal's story (_**here are parts [**_one_**](https://datafu.apache.org/blog/2019/01/29/a-look-at-paypals-contributions-to-datafu.html) and [**_two_**](https://datafu.apache.org/blog/2021/11/18/introducing-datafu-spark.html)**_)._**

Our latest release of _datafu-spark_ contained a new API, _collectNumberOrderedElements_, that was contributed by PayPal for dealing with tables with extreme skew. It is basically an optimized replacement for doing a _groupBy_ followed by _collect_list_, _array_sort_ and _slice_. In this post we will explain how we implemented this, when this method is useful, and what kind of savings you can expect when using it. 

---

Let's start by assuming we have a dataframe _df_ with the following schema:

```
root
 |-- account_id: string (nullable = true)
 |-- element_id: string (nullable = true)
 ```

 What we want is to take this dataframe, and convert it into a table in which each row represents a single account with a list of elements, but we only want a certain amount of elements per account - not more than a given constant. We also want the elements chosen to be determinstic - choose the first X elements after sorting. We can write Spark code that does this as follows:
 
 ```
val topFiveElementsPerAccount = df.groupBy("account_id").
      agg(collect_list("element_id").as("elements")).
      withColumn("elements", array_sort(col("elements")).as("elements")).
      withColumn("elements", slice(col("elements"), 1, 5))
 ```

We can simplify this by using _collectNumberOrderedElements_ in the following manner:

```
val topFiveElementsPerAccount = df.groupBy("account_id").agg(SparkOverwriteUDAFs.collectNumberOrderedElements(col("element_id"), 5, ascending = false).as("list"))
```

Let's simplify our example by asking a different question - how many accounts have above a certain amount of elements? We could add the following line to both versions above to find this out, assuming our cutoff value is 5:

```
val numberOfAccountsWithAtLeastFiveElements = topFiveElementsPerAccount.filter("size(elements) > 4").count
```

Our first table will have the following characteristics - about 300 million accounts, with the "biggest" account having about 500 elements. How long will this code take to run? (all our experiments will use a small cluster of 8 machines with 80GB memory and 16 cores). The naive Spark version takes about 24 seconds, and the DataFu variant takes 20. That's not long, and the difference isn't that big. But what if we have more skew? If we add up to a million records with the same _account_id_  it doesn't change the time it takes for either one to run. But if we increase the skew more we will see that the runtime of the naive solution increases. For example, with 10 million additional records with the same _account_id_, the runtime rises to around 44 seconds.  With 50 million additional records, the naive run time rises to 160. With 100 million, 340 seconds. At 150 million records the naive solution can no longer succeed - no executor can fit all these records in memory. The DataFu solution, on the other hand, continues to run in 20-23 seconds. (you can see all the runtimes in the table at the end of this post)
 
Another approach worth trying in similar cases is window functions. A window function allows reducing unneeded rows earlier than the naive groupBy solution, though less aggressively than _collectNumberOrderedElements_. What if we use a window function to reduce the number of records before we collect them into an array? How long would that take? We could write code like this:
 
```
import org.apache.spark.sql.expressions.Window

val win = Window.partitionBy($"account_id", $"element_id").orderBy($"element_id".desc, $"element_id")

df.select("account_id","element_id").withColumn("rank", row_number().over(win)).filter("rank <= 5").groupBy("account_id").
      agg(collect_list("element_id").as("elements")).
      withColumn("elements", array_sort(col("elements")).as("elements")).
      filter("size(elements) > 4").count
```
      
For up to a million skewed records, it runs in about 38 seconds - the worst of the three solutions. With 10 million skewed records, however, it beats the naive solution at 43 seconds. At 50 and 100 million records it rises to 78 and 113 seconds, respectively - still better. But at 150 million skewed records it too fails. 


| Skewed Records (same account_id) | Naive Solution Runtime | Window Function Runtime | DataFu Runtime |
|----------------------------------|------------------------|-------------------------|----------------|
| 0 (baseline: 500 max)            | 24 seconds             | 38 seconds              | 20 seconds     |
| 1 million                        | 24 seconds             | 38 seconds              | 20 seconds  |
| 10 million                       | 44 seconds             | 43 seconds              | 20 seconds  |
| 50 million                       | 160 seconds            | 78 seconds              | 21 seconds  |
| 100 million                      | 340 seconds            | 113 seconds             | 22 seconds  |
| 150 million                      | FAILED                 | FAILED                  | 23 seconds  |

What about when the total number of records rises? As expected, if there is no excessive skew the runtime of all three solutions rises linearly. In our experiments, 800 million records without skew took an average of 42 seconds with DataFu, 110 seconds with the naive solution, and 160 seconds with the window function.

Why is _collectNumberOrderedElements_  more efficient? Part of the answer is maintaining an internal buffer that stores at most N sorted elements throughout the aggregation process (see [SparkOverwriteUDAFs.scala:200-219](https://github.com/apache/datafu/blob/main/datafu-spark/src/main/scala/spark/utils/overwrites/SparkOverwriteUDAFs.scala#L200) or the diagram below), applying the sort-and-slice logic during both the update phase when processing new elements and the merge phase when combining partial results.

```
┌─────────────────────────────────────────────────────────────────┐
│                 UPDATE PHASE (per partition)                    │
│                                                                 │
│  Buffer: [A, C, E, G, I]  ←─── New element: D                   │
│          (sorted, size=5)                                       │
│                                                                 │
│  Step 1: Concat → [A, C, E, G, I, D]                            │
│  Step 2: Sort  → [A, C, D, E, G, I]                             │
│  Step 3: Slice → [A, C, D, E, G]  (take first N)                │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                 MERGE PHASE (across partitions)                 │
│                                                                 │
│  Buffer 1: [A, C, D, E, G]     Buffer 2: [B, F, H, J, K]        │
│                                                                 │
│  Step 1: Concat → [A, C, D, E, G, B, F, H, J, K]                │
│  Step 2: Sort  → [A, B, C, D, E, F, G, H, J, K]                 │
│  Step 3: Slice → [A, B, C, D, E]  (take first N)                │
│                                                                 │
│  Final Result: [A, B, C, D, E]                                  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```
 
It was implemented using Spark's (internal) _DeclarativeAggregate_ framework. This allows the Catalyst optimizer to eliminate unnecessary data as early as possible, even more aggresively than if it had been implemented using the public [Aggregator](https://spark.apache.org/docs/latest/sql-ref-functions-udf-aggregate.html) interface or using window function.
  
In conclusion - even if your data doesn't have any skew, the DataFu _collectNumberOrderedElements_ outperforms both a regular groupBy and using window functions. When extreme skew is present, it becomes not only faster but the only viable option. 

---
<br>

(A version of this post also appears in the [Technology At PayPal Blog](link-to-be-added-later)
 
