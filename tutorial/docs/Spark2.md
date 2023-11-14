# Page-rank algorithm 

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
    """
    This function takes elements from the joined dataset above and
    computes the contribution to each outgoing link based on the
    current rank.
    """
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
    """
    This function takes elements from the joined dataset above and
    computes the contribution to each outgoing link based on the
    current rank.
    """
    _, (urls, rank) = node_urls_rank
    nb_urls = len(urls)
    for url in urls:
        yield url, rank / nb_urls

def get_page_rank(links_data, beta=0.85, niter=10):

    xs = sc.parallelize(links_data)
    links = xs.groupByKey().mapValues(list)
    Nodes = (xs.keys() + xs.values()).distinct()
    ranks = Nodes.map(lambda x: (x, 100))

    for iteration in range(10):
        contribs = links.join(ranks).flatMap(computeContribs)
        contribs = links.fullOuterJoin(contribs).mapValues(lambda x : x[1] or 0.0)
        ranks = contribs.reduceByKey(add)
        ranks = ranks.mapValues(lambda rank: rank * beta + (1-beta))
    
    return ranks

```

```python
page_ranks = get_page_rank(mini_mathes)
r = sorted(page_ranks.collect())

page_rank = [x[1]*1000 for x in r]
G = nx.DiGraph()
G.add_edges_from(mini_mathes)
nx.draw_networkx(G, node_size=page_rank, node_color=page_rank)
```


HERE goes figure
### PageRank for tennis dataset
Now we are able to compute page rank of any graph. We'll preprocess the wta matches dataset:

```python 
def get_loser_winner(match):
    ms = match.split(',')
    return (ms[18], ms[10])

match_data = sc.textFile("data/tennis_wta-master/wta_matches*") 
xs = match_data.map(get_loser_winner) #rdd

page_rank = get_page_rank(links, beta=0.85, niter=10)

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
