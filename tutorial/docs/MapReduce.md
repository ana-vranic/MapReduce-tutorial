# Hadoop 

Apache Hadoop is an open-source implementation of a distributed MapReduce system.A Hadoop cluster consists of a name node and a number of data nodes. The name node holds the distributed file system metadata and layout, and it organizes executions of jobs on the cluster. Data nodes hold the chunks of actual data and they execute jobs on their local subset of the data.

Users define their jobs as map, reduce and optionally combine functions which they submit for the execution, along with the locations of input and output files, and optional parameters regarding the number of reducers and similar.

Hadoop was developed in Java and its primary use is through Java API, however Hadoop also offers a “streaming” API, which is more general and it can work with map-reduce jobs written in any language which can read data from standard input and return data to standard output. In this tutorial we will provide examples in Python. 

In terms of unix commands, this is equivalent to the following:
```sh
$ cat input.dat | mapper | sort | reducer > output.dat
```

where the mapper program (or script) takes each line of the input file, splits it into records according to its own rules, and derives one or multiple key-value pairs from it. It then outputs those key-value pairs to standard output which is then piped to sort for sorting. Reducer program takes sorted key-value pairs, each on a separate line on standard input, and performs a reduction operation on adjacent pairs which have the same key, outputing the final result for each key to its standard output.

This can be only one step of the analysis wich can go through as many iterations of map-reduce as needed. Also the system will scale the paralellism of the run according to available resources and distribution of the data set chunks on the data nodes.

### Python Streaming 

If you prefer to use languages other than Java, Hadoop offers the streaming API. The term streaming here refers to the way Hadoop uses standard input and standard output streams of your non-java mapper and reducer programs to pipe data between them. Relying on stdin and stdout for this enables easy integration with any other language.

The general syntax of the command to run streaming jobs is the following:

mapred streaming [genericOptions] [streamingOptions]

In the first example we will implement our word-count example. We could define our mapper in just a few lines of python:

```py title="map.py"
!/usr/bin/env python3
import sys
import re

WORD_REGEX = re.compile(r"[\w]+")

for line in sys.stdin:
    for word in WORD_REGEX.findall(line):
        print( word.lower(), 1)

```


This mapper will read text from standard input and for each word it will echo that word and the number 1 as the number of times the word appeared. The reducer will take those key-value pairs and sum the number of appearances for each word:

```py title="reduce.py"
#!/usr/bin/env python3
import sys

prevWord = ''
prevCount = 0
for line in sys.stdin:
    word, count = line.split()
    count = int(count)
    if word == prevWord:
        prevCount += 1
        continue
    else:
        if prevWord != '':
            print(prevWord, prevCount)
        prevWord = word
        prevCount = 1
if prevWord != '':
    print(prevWord, prevCount)
```
The reducer is a bit more complicated as it must keep track of which key it is reducing on. This happens because it will get all the values for a single key on a separate key-value line, i.e. hadoop streaming mechanism won’t automatically collect all the values for a key into an array of values. However, even with this hinderance, the code is still quite concise.

If we want to test our code in local on smaller dataset, in this case the Mobi Dick books, it can be run as:

```
cat pg2701.txt | python map.py | sort | python reduce.py >  word_count.dat
```

When moving to Hadoop cluster, the the scripts stay same and we can run it as. 
```
$ mapred streaming -input /user/<USERNAME>/2701.txt -output streamedwcs -mapper mapper.py -reducer reducer.py -file mapper.py -file reducer.py
```
The input and output parameters specify the locations for input and output data on the HDFS file system - Hierarchical distributed file system. Mapper and reducer parameters specify the mapper and reducer programs respectively. The following file parameters specify files on the local file system that will be uploaded to Hadoop and made available in the context of that job. Here we specify our python scripts in order to make them available for execution on the data nodes.

### Calculating ELO ratings

### Dataset download
We can download data from [repository](https://github.com/JeffSackmann/tennis_wta) 

``` sh
wget "https://github.com/JeffSackmann/tennis_wta/archive/refs/heads/master.zip"
```
The dataset contains `.csv files` with wta_matches from 1968 until 2023. 

For given tennis data over the several years of matches, we will calculate the [ELO ratings](https://stanislav-stankovic.medium.com/elo-rating-system-6196cc59941e) rating of each player, using MapReduce. 

The ELO ratings are disagned to reflect the level of player's skill. After each match the ELO rating of each player A is updated by formula 

$$
R^{'}_a = R_a + K(S_a-E_a)
$$

- $R^{'}_a$ is new rating, 
- $R_a$ is previous rating, 
 
- $S_a$ is actual outcome of the match. The actual outcome of one player may be victory $S_a=1$ or loss $S_a=0$ so $0<=S_a<=1$. 
- $E_a$ is expected outcome of the match while. To map the expectation of the outcome from 0 to 1, we can use logistic curve, so E_a = Q_a / (Q_a+Q_b), where $Q_a = 10^(R_a/c)$, $Q_b = 10^(R_a/c)$. The factor c can be 400.
- Parameter K is scaling factor, which determines how much influence each match can have, and in this example can be set on 1000 .

### The mapper

Each line in data contains attributes about the match such as winner, loser, surface, and for our purpose we can focus on selecting these three features. To select these elements, we need to split each line on comas, and select 2nd, 10th and 20th position. And we'll pass these features in the key-value pairs into json string, and result can be printed to standard output using json.dumps function from the JSON module. 
Mapper for analyzing tennis score

``` py title="elo_map.py"
#!/usr/bin/python3
import json
from sys import stdin

def clean_match(match):
    ms = match.split(',')
    match_data = {'winner': ms[10],
                 'loser':ms[18],
                  'surface': ms[2]
                 }
    return match_data

if __name__=="__main__":
    for line in stdin:
        print(json.dumps(clean_match(line)))
```

### The reducer

The reducer as input takes JSON objects, from standard input. In elo_acc function for each match we update the ratings of users and store them into dictionary acc. Also the obtained values are round to 5 decimals using round5 function. 

``` py title="elo_reduce.py"
#!/usr/bin/python3
import json
from sys import stdin
from functools import reduce

def round5(x):
    return 5*int(x/5)

def elo_acc(acc, nxt):

    match_info = json.loads(nxt)
    w_elo = acc.get(match_info['winner'], 1400)
    l_elo = acc.get(match_info['loser'], 1400)
    Qw = 10**(w_elo/400)
    Ql = 10**(l_elo/400)
    Qt = Qw+Ql

    acc[match_info['winner']] = round5(w_elo + 100*(1-(Qw/Qt)))
    acc[match_info['loser']] = round5(l_elo - 100*(Ql/Qt))
    return acc

if __name__=="__main__":
    xs = reduce(elo_acc, stdin, {})
    for player, rtg in list(xs.items())[:10]:
        print(rtg, player)
```
We can always check the output of our scripts in local: 

``` sh
cat mini_data.csv python mapper.py | sort | reducer.py
```
after running the output is:

``` json
"Julia Helbet": 1360,
"Glenny Cepeda": 1400,
"Hana Sromova": 1075,
"Sophie Ferguson": 1130,
"Anne Mall": 1360,
"Nuria Llagostera Vives": 1120,
"Maria Vento Kabchi": 1050,
"Roxana Abdurakhmonova": 1380,
"Zarina Diyas": 1405,
"Stephanie Vogt": 1430,
"Soumia Islami": 1390,
"Pei Ling Tong": 1380,
"Shikha Uberoi": 1160,
"Amani Khalifa": 1410,
...
```
With smaller dataset it is advisable to test our scripts on local computer, but when it comes to big data, the same script can be used with Hadoop cluster.  Hadoop streaming command to run elo rating calculator would be: 

``` sh
$HADOOP/bin/hadoop jar /home/<user>/bin/hadoop/hadoop-streaming-3.2.0.jar \
-file ./elo-mapper.py -mapper ./elo-mapper.py \
-file ./elo-reducer.py -reducer ./elo-reducer.py \
-input '/path/to/wta/files/wta_matches_200*.csv' \
-output ./tennis_ratings
```