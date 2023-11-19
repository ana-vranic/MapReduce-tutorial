# Hadoop 

Apache Hadoop is an open-source implementation of a distributed MapReduce system. A Hadoop cluster consists of a name node and a number of data nodes. The name node holds the distributed file system metadata and layout, and it organizes executions of jobs on the cluster. Data nodes hold the chunks of actual data and execute jobs on their local subset of the data.

Hadoop was developed in Java, and its primary use is through Java API; however, Hadoop also offers a “streaming” API, which is more general and it can work with map-reduce jobs written in any language which can read data from standard input and return data to standard output. In this tutorial, we will provide examples in Python. 

## Python Streaming 

If you prefer languages other than Java, Hadoop offers the streaming API. The term streaming here refers to how Hadoop uses standard input and output streams of your non-java mapper and reducer programs to pipe data between them. Relying on stdin and stdout enables easy integration with any other language.

In the first example, we will implement word count.
The mapper is defined as following: 

```py title="map.py"
!/usr/bin/env python3
import sys
import re

WORD_REGEX = re.compile(r"[\w]+")

for line in sys.stdin:
    for word in WORD_REGEX.findall(line):
        print( word.lower(), 1)

```
This mapper will read text from standard input, and for each word, it will echo that word and the number 1 as the number of times the word appeared. The reducer will take those key-value pairs and sum the number of appearances for each word:

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
The reducer is more complicated as it must keep track of which key it is reducing on. This happens because it will get all the values for a single key on a separate key-value line, i.e. hadoop streaming mechanism won’t automatically collect all the values for a key into an array of values. 

If we want to test our code locally,  on the Mobi Dick book, it can be run as:

```sh
cat pg2701.txt | python map.py | sort | python reduce.py >  word_count.dat
```


When moving to the Hadoop cluster, the scripts map.py and reduce.py stay the same, and we can run it as. 
```sh
$ mapred streaming  \
-input "2701.txt" \
-output "word_count.dat" \
-mapper mapper.py -reducer reducer.py \ 
-file mapper.py -file reducer.py
```
The input and output parameters specify the locations for input and output data on the HDFS file system - Hierarchical distributed file system. Mapper and reducer parameters specify the mapper and reducer programs, respectively. The following parameters specify files on the local file system that will be uploaded to Hadoop and made available in the context of that job. Here, we define our python scripts to make them available for execution on the data nodes.

## Calculating ELO ratings

We can download data from [repository](https://github.com/JeffSackmann/tennis_wta) 

``` sh
wget "https://github.com/JeffSackmann/tennis_wta/archive/refs/heads/master.zip"
```
The dataset contains `.csv files` with wta_matches from 1968 until 2023. 

For given tennis data over several years of matches, we will calculate the [ELO ratings](https://stanislav-stankovic.medium.com/elo-rating-system-6196cc59941e) of each player, using MapReduce, which should reflect the player's relative skills. After each match, we update the ratings of players. The ELO rating of player A is updated by formula: 

$$
R^{'}_a = R_a + K(S_a-E_a)
$$

- $R^{'}_a$ is new rating, 
- $R_a$ is previous rating, 
 
- $S_a$ is actual outcome of the match. The actual outcome of one player may be victory $S_a=1$ or loss $S_a=0$ so $0\leq S_a \leq 1$. 
- $E_a$ is expected outcome of the match while. To map the expectation of the outcome from 0 to 1, we can use logistic curve, so $E_a = Q_a / (Q_a+Q_b)$, where $Q_a = 10^{R_a/c}$, $Q_b = 10^{R_b/c}$. The factor c can be 400.
- Parameter K is scaling factor, which determines how much influence each match can have, and in this example can be set on $100$ .

The mapper:

Each line in data contains attributes about the match such as winner, loser, surface. To select these elements, we need to split each line on comas, and select 2nd, 10th and 20th position. And we'll pass these features in the key-value pairs into json string, and result can be printed to standard output using json.dumps function from the JSON module. 

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

The reducer:

The reducer takes JSON objects, from standard input. In elo_acc function for each match we update the ratings of users and store them into dictionary acc. Also the obtained values are round to 5 decimals using round5 function. 

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
    # return dictionary
    xs = reduce(elo_acc, stdin, {})
    topN = (sorted(xs.items(), key=lambda item: item[1], reverse=True))[:20]
    
    for player, rtg in topN:
        print(rtg, player)
 

```
We can always check the output of our scripts in local: 

``` sh
cat tennis_wta-master/wta_matches_* | python elo_map.py | sort | python elo_reduce.py 

```
after running script we'll get result similar to this:

``` json
3075 Zina Garrison
2930 Victoria Azarenka
2845 Zarina Diyas
2765 Zuzana Ondraskova
2745 Yung Jan Chan
2745 Yvonne Vermaak
2710 Tatiana Golovin
2700 Yurika Sema
...
```
To run it on Hadoop cluster: 

``` sh
$ mapred streaming \
-file ./elo-mapper.py -mapper ./elo-mapper.py \
-file ./elo-reducer.py -reducer ./elo-reducer.py \
-input '${path}/wta_matches_200*.csv' \
-output "tennis_ratings"
```