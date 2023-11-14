## PySpark

Apache Spark is another popular cluster computing framework for big data processing. Contrary to Hadoop it takes advantage of high RAM compute machines, which are now available. It means that Spark processes data in memory on the distributed network instead of storing data in the filesystem. This can impove the processing time. The advantages of Spark are that it natively supports programing languages like Scala, Java, Python, R. Spark has direct python interface - **pyspark**, which uses same analogy of map and reduce. It can be used interactivly from command shell or jupyter notebook. Spark can query SQL databases directly, and it also  DataFrame API similar to pandas. 

Instalation of pyspark requires installed java. From there we can istall pyspark as any other python package and test our scrips localy before moving to Spark cluster. 

```
pip install pyspark
```
Before we introduce main features of the pyspark, let's see how we can write the simplest problem word count.

### Word Count in PySpark


```python title="word_count_spark.py"
from pyspark import SparkContext

def main():
  sc = SparkContext(appName='SparkWordCount')
  input_file = sc.textFile('pg2701.txt')
  counts = input_file.flatMap(lambda line: line.split()) \
  .map(lambda word: (word, 1)) \
  .reduceByKey(lambda a, b: a + b)
  counts.saveAsTextFile('output')
  sc.stop()

if __name__ == '__main__':
  main()

```
The code can be run with  ```spark-submit word_count_spark.py``` 
the output will apear in folder output/0001 and outpu/0002, depending on the number of processes.

The ```sc = SparkContext(appName='SparkWordCount')``` creates context object, which tells Spark how to acess the cluste. 
The ```input_file = sc.textFile('pg2701.txt')``` loads data. 
The third line performs multiple transformations of input data, similar as we had before. Everything is automaticly parallelized and run across multiple nodes. 

### Resilient Distributed Datasets (RDDs)

RDDs are immutable collections of data distributed across machines, which enables that operations are performed in parallel. They can be created from collections by calling  ```parallelize()``` method:

```python
data = [1, 2, 3, 4, 5, 6]
rdd = sc.parallelize(data)
rdd.glom().collect()
```
RDD.glom() returns a list of elements within each partition, while RDD.collect() collect all elements to the driver node. 
This specifies the number of partitions:
```
rdd = sc.parallelize(data, 4)
rdd.glom().collect()
```

Another way to create Rdd is from file using ```textFile()``` method, as we did in the word count example. 
 

### RDD Operations

Usually we create first RDDs from some data, then apply some dataset tranformation 

```python
lines = sc.textFile('data.txt')
line_lengths = lines.map(lambda x: len(x))
document_length = line_lengths.reduce(lambda x,y: x+y)
print(ocument_length)
```
The first statement creates an RDD from the external file data.txt. This file is not loaded at this point; the variable lines is just a pointer to the external source. The second statement performs a transformation on the base RDD by using the map() function to calculate the number of characters in each line. The variable line_lengths is not immediately computed due to the laziness of transformations. Finally, the reduce() method is called, which is an action. At this point, Spark divides the computations into tasks to run on separate machines. Each machine runs both the map and reduction on its local data, returning only the results to the driver program. With lambda function we can easy pass the function which has to be run on the cluster. 

Here are listed some commonly used operations which can be applied on the RDDs: 

- map() map. function returns a RDD by applying function to each element of the source RDD


```python 
data = [1, 2, 3, 4, 5, 6]
rdd = sc.parallelize(data)
map_result = rdd.map(lambda x: x * 2)
map_result.collect()
[2, 4, 6, 8, 10, 12]
```

- flatMap() returns a flattened version of results. 

```python
data = [1, 2, 3, 4]
>>> rdd = sc.parallelize(data)
>>> map = rdd.map(lambda x: [x, pow(x,2)])
>>> map.collect()
[[1, 1], [2, 4], [3, 9], [4, 16]]``

```

```python
rdd = sc.parallelize()
flat_map = rdd.flatMap(lambda x: [x, pow(x,2)])
flat_map.collect()
[1, 1, 2, 4, 3, 9, 4, 16]
```

- .groupBy()

``` python
xs = sc.parallelize(["apple", "banana", "cantaloupe"])
xs.groupBy(getFirstLetter)
[("a",["apple"]), ("b", ["banana"]), ("c", ["cantaloupe"])]
```

- .groupByKey() python
  
```
xs = sc.parallelize([("pet", "dog"), ("pet", "cat"),
("farm", "horse"), "farm", "cow")])
xs.groupByKey()
[("pet", ["dog", "cat"]), ("farm", ["horse", "cow"])]
```

- filter(func) returns a new RDD contains only elements that function return as true

```python
data = [1, 2, 3, 4, 5, 6]
filter_result = rdd.filter(lambda x: x % 2 == 0)
filter_result.collect()
[2, 4, 6]
```

- distinct. this returns unique elements in the list

```
data = [1, 2, 3, 2, 4, 1]
rdd = sc.parallelize(data)
distinct_result = rdd.distinct()
distinct_result.collect()
[4, 1, 2, 3]
```

- reduce

- reduceByKey

## Page-rank algorithm 

The page-rank was used as Google's ranking systems, resulting that websites with higher PageRank score would show up higher in Google search. PageRank can be performed on the graph (network) structured datasets. PageRank will rank nodes, giving the ranking of nodes by their influence. The more followers node has, the more influential, and more those followers are influential the more they will contribute to the node's rank.The more details about PageRank algorithm can be found in the original paper. 

### PageRank in pyspark

We will perform PageRank on wta matches dataset used before, but before we apply PageRank on the whole dataset, we will try to understand implementation in the pyspark on the small dataset.

<center>

| match | loser      | winner  |
| :----:| :--------: | ----:   |
| 1     |  player1   | player2 |
| 2     |  player2   | player3 |
| 3     |  player3   | player4 |
| 4     |  player2   | player1 |
| 5     |  player4   | player2 |

</center>

```python
import networkx as nx
mini_mathes = [('player1', 'player2'), 
              ('player2', 'player3'),
              ('player3', 'player4'),
              ('player2', 'player1'),
              ('player4', 'player2')]


G = nx.DiGraph()
G.add_edges_from(mini_mathes)
nx.draw_networkx(G)
```
![Players in graph representation](graph1.png)

```python
import pyspark
xs = sc.parallelize(mini_mathes)
links = xs.groupByKey().mapValues(list)
links.collect()
-----------------------
[('player1', ['player2']),
 ('player2', ['player3', 'player1']),
 ('player3', ['player4']),
 ('player4', ['player2'])]
```

```python
Nodes = (xs.keys() + xs.values()).distinct()
ranks = Nodes.map(lambda x: (x, 100))
sorted(ranks.collect())
--------------------------
[('player1', 1.0), ('player2', 1.0), ('player3', 1.0), ('player4', 1.0)]

```

```python 
links.join(ranks).collect()
-------------------------------------
links.join(ranks).collect()

[('player2', (['player3', 'player1'], 1.0)),
 ('player3', (['player4'], 1.0)),
 ('player4', (['player2'], 1.0)),
 ('player1', (['player2'], 1.0))]
```

We are going to compute contributions of each node as 

```
from operator import add

def computeContribs(node_urls_rank):
    _, (urls, rank) = node_urls_rank
    nb_urls = len(urls)
    for url in urls:
        yield url, rank / nb_urls

for iteration in range(10):
    contribs = links.join(ranks).flatMap(computeContribs)
    contribs = links.fullOuterJoin(contribs).mapValues(lambda x : x[1] or 0.0)
    ranks = contribs.reduceByKey(add)
    ranks = ranks.mapValues(lambda rank: rank * 0.85 + 0.15)

    print(sorted(ranks.collect()))

```

Lets bring everything together:

```python 
from operator import add

def computeContribs(node_urls_rank):
    _, (urls, rank) = node_urls_rank
    nb_urls = len(urls)
    for url in urls:
        yield url, rank / nb_urls

def get_page_rank(xs, beta=0.85, niter=10):

    
    links = xs.groupByKey().mapValues(list)
    Nodes = (xs.keys() + xs.values()).distinct()
    ranks = Nodes.map(lambda x: (x, 100))

    for iteration in range(10):
        contribs = links.join(ranks).flatMap(computeContribs)
        contribs = links.fullOuterJoin(contribs).mapValues(lambda x : x[1] or 0.0)
        ranks = contribs.reduceByKey(add)
        ranks = ranks.mapValues(lambda rank: rank * beta + (1-beta))
    
    return ranks
xs = sc.parallelize(mini_matches)
page_ranks = get_page_rank(xs)
r = sorted(page_ranks.collect())

page_rank = [x[1]*1000 for x in r]
G = nx.DiGraph()
G.add_edges_from(mini_mathes)
nx.draw_networkx(G, node_size=page_rank, node_color=page_rank)
```
![Image title](graph2.png)


### PageRank for tennis dataset
Now we are able to compute page rank of any graph. We'll preprocess the wta matches dataset:

```python 
def get_loser_winner(match):
    ms = match.split(',')
    return (ms[18], ms[10])

match_data = sc.textFile("data/tennis_wta-master/wta_matches*") 
xs = match_data.map(get_loser_winner) #rdd

page_rank = get_page_rank(xs, beta=0.85, niter=10)

sorted(page_rank.collect(), key=lambda x: x[1], reverse=True)[:10]

```

When we execute it we'll get the most influential tennis players according to PageRank measure:
```sql
[('Martina Navratilova', 5474.833387989397),
 ('Chris Evert', 4636.296862841747),
 ('Steffi Graf', 3204.060329739364),
 ('Serena Williams', 3039.4891463181716),
 ('Venus Williams', 2737.6910644598042),
 ('Lindsay Davenport', 2544.223902071716),
 ('Billie Jean King', 2258.2918906684013),
 ('Arantxa Sanchez Vicario', 2113.238892322328),
 ('Virginia Wade', 2064.516569589297),
 ('Monica Seles', 2028.8803038982473)]

```
## Machine Learning in PySpark
Apache Spark offers a Machine Learning API called MLlib. PySpark has this machine learning API in Python as well. It supports different kind of algorithms, which are mentioned below −

   - mllib.classification − The spark.mllib package supports various methods for binary classification, multiclass classification and regression analysis. Some of the most popular algorithms in classification are Random Forest, Naive Bayes, Decision Tree, etc.

   - mllib.clustering − Clustering is an unsupervised learning problem, whereby you aim to group subsets of entities with one another based on some notion of similarity.

   - mllib.fpm − Frequent pattern matching is mining frequent items, itemsets, subsequences or other substructures that are usually among the first steps to analyze a large-scale dataset. This has been an active research topic in data mining for years.

   - mllib.linalg − MLlib utilities for linear algebra.

  -  mllib.recommendation − Collaborative filtering is commonly used for recommender systems. These techniques aim to fill in the missing entries of a user item association matrix.

   - spark.mllib − It ¬currently supports model-based collaborative filtering, in which users and products are described by a small set of latent factors that can be used to predict missing entries. spark.mllib uses the Alternating Least Squares (ALS) algorithm to learn these latent factors.

   - mllib.regression − Linear regression belongs to the family of regression algorithms. The goal of regression is to find relationships and dependencies between variables. The interface for working with linear regression models and model summaries is similar to the logistic regression case.

There are other algorithms, classes and functions also as a part of the mllib package. As of now, let us understand a demonstration on pyspark.mllib.




