## Datasets

- [Moby Dick book](https://nyu-cds.github.io/python-bigdata/files/pg2701.txt), we will get file ```pg2701.txt```

``` sh
wget "https://gutenberg.org/cache/epub/2701/pg2701.txt"
```

- Tennis WTA matches can be downloaded from the github [repository](https://github.com/JeffSackmann/tennis_wta) 

``` sh
wget "https://github.com/JeffSackmann/tennis_wta/archive/refs/heads/master.zip"
unzip master.zip
```
The dataset contains `.csv files` with WTA matches from 1968 until 2023. 

- Iris Flowers Dataset can be downloaded from many sources, in this tutorial I used one from [Kaggle](https://www.kaggle.com/datasets/himanshunakrani/iris-dataset)



## Requirement

- python3
- mrjob ```pip install mrjob```
- pyspark ```pip install pyspark``` To use pyspark you need previosly installed java. 
- networkx ```pip install networkx```.
- matplotlib ```pip install matplotlib```
- pandas ```pip install pandas```

## Literature
- [MapReduce: Simplified Data Processing on Large Clusters](https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf)
-  Page Rank paper, [The PageRank Citation Ranking: Bringig Order to the Web](https://www.cis.upenn.edu/~mkearns/teaching/NetworkedLife/pagerank.pdf)

- Wolohan, J. (2020). Mastering Large Datasets with Python: Parallelize and Distribute Your Python Code. United States: Manning.
-  Radtka, Z., & Miner, D. (2015). Hadoop with Python. O'Reilly Media.
-  Tutorial [BigData with PySpark](https://nyu-cds.github.io/python-bigdata/) by New York University 
-  CornellEdu [BigData Technologies](https://people.orie.cornell.edu/vc333/orie5270/lectures/01/) course

- [Paradox Hadoop user guide](https://www.scl.rs/PARADOX_User_Guide/hadoop-user-guide.html)

