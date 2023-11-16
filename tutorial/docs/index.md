# Introduction

In Data Science, we often deal with big amounts of data. In those cases, many standard approaches won't work as expected, and to process big data, we need to apply a different technique called MapReduce. The main problem when dealing with big data is that the data size is so large that filesystem access times become a dominant factor in the execution time. Because of that, it is not efficient to process big data on a standard MPI cluster machines. With distributed computing solutions  like Hadoop and Spark clusters, which rely on the MapReduce approach, big volumes of data are processed and created by diving work into independent tasks, performing the job in parallel. For the first time, the MapReduce approach was formalized by Google, in the paper [MapReduce: Simplified Data Processing on Large Clusters](https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf) when they encountered a problem in indexing all pages on the WWW. 

!!! note "map() and reduce() in python"

    Before we further explain the MapReduce approach, we will review two functions from Python, ```map()``` and ``reduce()``, introduced in functional programming. In imperative programming, computation is carried through statements, where the execution of code changes the state of variables. Unlike in functional programming, the state is no longer necessary because functional programming works on immutable data types; functions create and work on new data sets while the original dataset is intact. 

    - map() applies the function to each element in the sequence and returns the resulting sequence. map() also returns the memory address of the returned map generator object and has to be called with a list() or through the loop. 

    ``` 
    >> ls = list(range(10))
    >> list(map(lambda x: x**2, ls))
    [1, 1, 4, 9, 16, 25, 36]

    ```

    - reduce() function returns a single value. It applies the function to the sequence elements from left to right. 

    ```
    from functools import reduce
    >>> ls = list(range(1, 10, 2))
    [1, 3, 5, 7, 9]
    >>> reduce(lambda x, y: x*y, ls) #(1*3*5*7*9)
    945

    ```


## Word Count

Counting the number of words in the documents is the simplest example when learning the MapReduce technique. In this tutorial, we will work on the [Moby Dick book](https://nyu-cds.github.io/python-bigdata/files/pg2701.txt). In python, we can use the following code:

```py title="word_count.py"
import re
WORD_REGEX = re.compile(r"[\w]+")

# remove any non-words and split lines into separate words
# finally, convert all words to lowercase

def splitter(line):
    line = WORD_REGEX.findall(line)
    return map(str.lower, line)

    
sums = {}
try:
    in_file = open('pg2701.txt', 'r')

    for line in in_file:
        for word in splitter(line):
            sums[word] = sums.get(word, 0) + 1
                 
    in_file.close()

except IOError:
    print("error performing file operation")
else:
    M = max([x for x in sums], key=lambda k: sums[k])
    print("max: %s = %d" % (M, sums[M]))

```
Ater running the program we will get:

```max: the = 14620```

A program written like this runs only on one processor, and we expect that the time necessary to process the whole text is proportional to the size of the text. Also, as the size of the dictionary grows, the performance degrades. When the size of the dictionary reaches the size of RAM or even swap space, the program will be stopped.  

In the map-reduce approach, we can avoid memory issues we may encounter. With a larger dataset, where we can still use our local computers, we can use modified versions of map-reduce functions. When data becomes big, we need to process and store data in distributed frameworks like Hadoop Spark or online computing services like AWS or Azure, where we can use the same principles and patterns we will learn in this tutorial. 

MapReduce consists of 3 steps:

- Map step which produces the intermediate results
- Shuffle step, which groups intermediate results with the same output key
- Reducing step that processes groups of intermediate results with the same key

![Image title](mrwc.png)

This approach works on data sets that consist of data records. The idea is to define the calculation in terms of two steps - a map step and a reduce step. The map operation is applied to every data record in the set and returns a list of key-value pairs. Those pairs are then collected, sorted by key, and passed into the reduce operation, which takes a key and a list of values associated with that key and then computes the final result for that key, returning it as a key-value pair.

## The outline 

To explore MapReduce further, this tutorial will cover MapReduce paradigm usage in different frameworks:

- Hadoop cluster streaming for python
- Python mrjob library
- Python library pyspark for pythonic usage of Spark cluster