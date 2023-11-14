# Introduction

In Data Science we often deal with huge amount of data. In those cases, many standard aproaches won't work as expected and in order to process big data we need to apply different techniqe called MapReduce. The main problem when dealing with big data is that size of the data is so large that filesystem access times become dominant factor in the execution time, and beacause of that it is not efficent to process big data on standard MPI cluster. With distributed computing solutions Hadoop and Spark clusters, which rely on MapReduce aproach, big volumes of data are processed and created by diving work into independent task, performing the task in parallel across a cluster of machines. The MapReduce approach was for the first time formalized by Google, when they encountered the problem in indexing all pages on the WWW. 

The MapReduce style of computing can be used in different context:

When dealing with data of normal size, we can use ```map()``` and ```reduce()``` functions from Python code. 
        In imperative programming — the more common programming paradigm you’re probably already familiar with, computation is carried out through statements. These consist of commands whose execution changes a variable’s value, and therefore the state of the computation. For example, a for loop may repeatedly execute a statement, each time changing the value of a variable. n contrast, functional programming eliminates the notion of state. Instead of changing the values of variables, functional programming works only on immutable data types. Because the values of immutable data types cannot be altered, we only work on the copies of the original data structure. Map, filter and reduce functions in Python work precisely in this manner — they create and work on new sets of data, leaving the original data intact.
        
- map() applies the function it receives as an argument to each element in a sequence and returns the resulting sequence. In the example below, we create a list of integers and using map() and Python’s str() function, we cast each integer into a string. The output is a list of strings. Notice how we enclose the result returned by the map() function within a list. We do this because in Python, map() returns the memory address of the returned map generator object.

``` 
>> ls = list(range(10))
>> list(map(lambda x: x**2, ls))
[1, 1, 4, 9, 16, 25, 36]

```

 - reduce()

Python’s reduce() function doesn’t return a new sequence like map() and filter(). Instead, it returns a single value. reduce() applies the function to the elements of the sequence, from left to right, starting with the first two elements in the sequence. We combine the result of applying the function to the sequence’s first two elements with the third element and pass them to another call of the same function. This process repeats until we reach the end of the iterable and the iterable reduces to a single value. 

```
from functools import reduce
>>> ls = list(range(1, 10, 2))
[1, 3, 5, 7, 9]
>>> reduce(lambda x, y: x*y, ls) #(1*3*5*7*9)
945

```

## Example

Counting the number of words in the documents is considered as simplest example when learning MapReduce techniqe. 
First we will download the book [Moby Dick] book.

In standard python we can use following code:

```py title="word_count.py"
import re

# remove any non-words and split lines into separate words
# finally, convert all words to lowercase
def splitter(line):
    line = re.sub(r'^\W+|\W+$', '', line)
    return map(str.lower, re.split(r'\W+', line))
    
sums = {}
try:
    in_file = open('pg2701.txt', 'r')

    for line in in_file:
        for word in splitter(line):
            word = word.lower()
            sums[word] = sums.get(word, 0) + 1
                 
    in_file.close()

except IOError:
    print("error performing file operation")
else:
    M = max([x for x in sums], key=lambda k: sums[k])
    print("max: %s = %d" % (M, sums[M]))

```
Ater running the program we will get:

```max: the = 14715```

Program written like this runs only on one processor and we will expect that time necessary to process whole text is proportional to the size of the text. Also, as the size of dictionary grow the performance degrades. When the size of dictionary reaches the size of RAM memory, or even swap space and program will be stopped.  

In map-reduce aproach we can avoid memory issues we may encouter as in previous example. With larger dataset, where we can still use our local computers, we can use modifed versions of map-reduce functions. When data become big we need to process and store data in distributed frameworks like Hadoop, Spark, or on online computing services like AWS or Azure, where we can use same principles and patterns we are going to learn in this tutorial.   

MapReduce consists of 3 steps:

- Map step which produces the intermediate results
- Shuffle step which groups intermidiate results with the same output key
- Reducing step that processes groups of intermidiate results with the same key

![Image title](mrwc.png)

This approach works on data sets wich consist of data records. The idea is to define the calculation in terms of two steps - a map step and a reduce step. The map operation is applied to every data record in the set and it returns a list of key-value pairs. Those pairs are then collected, sorted by key and passed into the reduce operation, which takes a key and a list of values associated with that key, and then computes the final result for that key, returning it also as a key-value pair.

## The outline 

To explore MapReduce further, this tutorial will cover MapReduce paradigm usage in different frameworks:

- Hadoop cluster streaming for python
- Python mrjob library
- Python library pyspark for pythonic usage of Spark cluster