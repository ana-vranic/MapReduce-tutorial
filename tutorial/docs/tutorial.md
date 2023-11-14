## Used datasets

- [Moby Dick book](https://nyu-cds.github.io/python-bigdata/files/pg2701.txt), we will get file ```pg2701.txt```

``` sh
wget "https://gutenberg.org/cache/epub/2701/pg2701.txt"
```

- Tennis wta matches can be downloaded from the github [repository](https://github.com/JeffSackmann/tennis_wta) 

``` sh
wget "https://github.com/JeffSackmann/tennis_wta/archive/refs/heads/master.zip"
```
The dataset contains `.csv files` with wta_matches from 1968 until 2023. 

## Requirments

- python3
- mrjob ```pip install mrjob```
- pyspark ```pip install pyspark``` To use pyspark you need previosly installed java. 
- networkx ```pip install networkx```.