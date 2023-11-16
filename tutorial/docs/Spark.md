## PySpark

Apache Spark is another popular cluster computing framework for big data processing. Contrary to Hadoop, it takes advantage of high-RAM computing machines, which are now available. Spark processes data in memory on the distributed network instead of storing data in the filesystem. This can improve the processing time. Spark's advantages are natively supporting programming languages like Scala, Java, Python, and R. Spark has a direct Python interface - **pyspark**, which uses the same analogy of map and reduce. It can be used interactively from the command shell or jupyter notebook. Spark can query SQL databases directly, and DataFrame API is similar to pandas. 

Installation of pyspark requires installed Java. From there, we can install pyspark as any other python package and test our scrips locally before moving to the Spark cluster. 

```
pip install pyspark
```
Before we introduce the main features of the pyspark, let's see how we can write the simplest problem word count.

**Word Count in PySpark**


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
the output will appear in folder output/0001 and output/0002, depending on the number of processes.

The ```sc = SparkContext(appName='SparkWordCount')``` creates a context object, which tells Spark how to access the cluster. 
The ```input_file = sc.textFile('pg2701.txt')``` loads data. 
The third line performs multiple input data transformations, similar to before. Everything is automatically parallelized and runs across multiple nodes. 


**Resilient Distributed Datasets (RDDs)**

RDDs are immutable collections of data distributed across machines, which enables operations to be performed in parallel. They can be created from collections by calling  ```parallelize()``` method:

```python
data = [1, 2, 3, 4, 5, 6]
rdd = sc.parallelize(data)
rdd.glom().collect()
```
RDD.glom() returns a list of elements within each partition, while RDD.collect() collect all elements to the driver node. 
To specify the number of partitions:
```
rdd = sc.parallelize(data, 4)
rdd.glom().collect()
```

Another way to create Rdd is from a file using ```textFile()``` method, as we did in the word count example. 
 

**RDD Operations**

Usually we create first RDDs from some data, then apply some dataset tranformation 

```python
lines = sc.textFile('data.txt')
line_lengths = lines.map(lambda x: len(x))
document_length = line_lengths.reduce(lambda x,y: x+y)
```
The first statement creates an RDD from the external file data.txt. This file is not loaded; the variable lines are just a pointer to the external source. The second statement transforms the base RDD using the map() function to calculate the number of characters in each line. The variable line_lengths is not immediately computed due to the laziness of transformations. Finally, the reduce() method is called an action. At this point, Spark divides the computations into tasks on separate machines. Each machine runs the map and reduction on its local data, returning only the results to the driver program. With the lambda function, we can easily pass the function that has to be run on the cluster. 

Here are listed some commonly used operations that can be applied to the RDDs: 

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
data = xs.groupBy(lambda x: x[0]).collect()
sorted([(x, sorted(y)) for (x, y) in data])
```
[('a', ['apple']), ('b', ['banana']), ('c', ['cantaloupe'])]

- .groupByKey() python
  
```
xs = sc.parallelize([("pet", "dog"), ("pet", "cat"),("farm", "horse"), ("farm", "cow")])
data = xs.groupByKey().collect()

[(x, list(y)) for (x, y) in data]
```
[('farm', ['horse', 'cow']), ('pet', ['dog', 'cat'])]

- filter(func) returns a new RDD contains only elements that function return as true

```python
data = [1, 2, 3, 4, 5, 6]
rdd = sc.parallelize(data)
filter_result = rdd.filter(lambda x: x % 2 == 0)
filter_result.collect()
```
[2, 4, 6]

- distinct. this returns unique elements in the list

```
data = [1, 2, 3, 2, 4, 1]
rdd = sc.parallelize(data)
distinct_result = rdd.distinct()
distinct_result.collect()
[4, 1, 2, 3]
```

- reduce

```py
data = [1, 2, 3]
rdd = sc.parallelize(data)
print(rdd.reduce(lambda a, b: a+b))
```
6

- reduceByKey

```py
data = [(1, 2), (1, 5), (2, 4)]
rdd = sc.parallelize(data)
rdd.reduceByKey(lambda a, b: a+b).collect()
```
[(1, 7), (2, 4)]

## Page-rank algorithm 

The PageRank was used as Google's ranking system, resulting in websites with higher PageRank scores showing up higher in Google searches. PageRank can be performed on the graph (network) structured datasets. PageRank will rank nodes, giving the ranking of nodes by their influence. The more followers the node has, the more influential it is, and the more those followers are influential, the more they will contribute to the node's rank. More details about the PageRank algorithm can be found in the original paper. 

Page rank can be calculated as:

$r_i = (1-d) + d(\sum_{j=1}^{N} I_{ij} \frac{r_j}{n_j} )$

The PageRank of a node is (1-dumping factor) + every node that points to node $i$ will contribute with page rank of node j/number of outgoing links.

**PageRank in pyspark**

We will perform PageRank on the WTA matches dataset used before. Before we apply PageRank on the whole dataset, we will try to understand implementation in the pyspark on the small dataset.

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

```py
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

Let us bring everything together:

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


**PageRank for tennis dataset**

Now, we can compute the page rank of any graph. We'll preprocess the WTA matches dataset:

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

Apache Spark offers a machine learning API called MLlib, where we can find a different kinds of machine learning algorithms, such as mllib.classification, mllib.linalg, mllib.recommendation, mllib.regression, mllib.clustering.  

Another usefull 

```py
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("mlwithspark").getOrCreate()
df = spark.read.csv('iris_dataset.csv', inferSchema=True, header=True)
```

```{.text .out}
150 5
```

```py
print(df.count(), len(df.columns))

df.columns

df.printSchema()

df.orderBy('species').show(10, False)

df.select('species').distinct().count()

df.groupBy('species').count().orderBy('count').show()

df.withColumn("petal_area", (df['petal_length']*df['petal_width'])).show()

df.filter(df['species']=='setosa').show()

df.groupBy('species').sum().show()

df.groupBy('species').max().show()

df.groupBy('species').min().show()
```

```
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
input_cols=['sepal_length', 'sepal_width', 'petal_length', 'petal_width']

vec_assembler = VectorAssembler(inputCols=input_cols, outputCol='features')
final_data = vec_assembler.transform(df)
from pyspark.ml.evaluation import ClusteringEvaluator

evaluator = ClusteringEvaluator(predictionCol='species', featuresCol='features',metricName='silhouette', distanceMeasure='squaredEuclidean')

errors=[]

for k in range(2,10):
    kmeans = KMeans(featuresCol='features',k=k)
    model = kmeans.fit(final_data)
    output = model.transform(final_data)
    errors.append( model.summary.trainingCost)
    print("With K={}".format(k))
    print("Within Set Sum of Squared Errors = " + str(errors))

```


```
import matplotlib.pyplot as plt
cluster_number = range(2, 10)
plt.plot(cluster_number, errors, 'o-')

kmeans = KMeans(featuresCol='features',k=3,)
model = kmeans.fit(final_data)
model.transform(final_data).groupBy('prediction').count().show()

predictions.groupBy('species','prediction').count().show()
```





