# mrjob

 mrjob is Python MapReduce library that wraps Hadoop streaming, and allows us to write the MapReduce programs in more pythonic manner. With mrjob it is possible to write multistep jobs. mrjob programs can be tested localy, they can be run on Hadoop cluster and can be run in the Amazon cloud using Amazon Elastic MapReduce (EMR).

- instalation ```$ pip install mrjob ```

In mrjob MapReduce function is defined as class MRClass which contains the methods which define the MapReduce job:

 - the mapper() defines the mapper. It takes (key, values) as arguments and yields tuppels (output_key, output_valies)

 - the combiner() defines the process that runs after mapper and before reducer. It recieves all data from mappper and the output of maper is sent to reducer. The combiner’s input is a key, which was yielded by the mapper, and a value, which is a generator that yields all values yielded by one mapper that corresponds to the key. The combiner yields tuples of (output_key, output_value) as output.
 
 - the reducer() defines the reducer for the MapReduce job. It takes key and an iterator of values as arguments and yields tuples of (outup_key, output_value)

- The final component is, which enables the execution of mrjob. 

``` py
if __name__ == '__main__':

    MRClass.run()
```

### word count example with mrjob

We will perform word count on Moby Dick book downloaded from ProjectGutenberg

``` sh
wget "https://gutenberg.org/cache/epub/2701/pg2701.txt"
```

mrjob script wordcount_mrjob.py: 

``` py title="mrjob_wc.py"
from mrjob.job import MRJob

WORD_REGEX = re.compile(r"[\w]+")
class MRWordCount(MrJob):

    def mapper(self, _, line):
        for word in WORD_REGEX.findall(line):
            yield word.lower(), 1


    def reducer(self, word, counts):
        yield(word, sum(counts))

    
if __name__ == '__main__':
    MRWordCount.run()

```

To run it localy:

``` sh
$ python wordcount_mrjob.py 'pg2701.txt'
```

We can execute the mrjob localy:  ```$ python mrjob.py input.txt```. The mrjob writes output to stout. 
We can also pass the multiple files ```$ python mrjob.py input.txt input2.txt input3.txt```

Finally, with ```-runner/-r``` option we can define how job executes. If job executes in Hadoop cluster  ```bash $ python mrjob.py -r hadoop input.txt``` If we run it on th EMR cluster  ```$ python mrjob.py -r emr s3://input-bucket/input.txt ```.

### chaining map-reduce

With mrjob we can easy chain several map-reduce functions. For example if we need to calculate the word with maximum frequency in the dataset. To do that we need to override the steps() method. The code will have mapper and reducer same as in previous task. Then, secound mapper uses the output of reducer, which map all (word, count) pairs to same key None The shuffle step of map-reduce will  collect them all into one list coresponding to key None. Then reducer_post will sort the list of (word, word_count) pairs by word_count and yield the word with maximum frequency. 

``` py title="mrjob_wf.py"
from mrjob.job import MRJob
from mrjob.step import MRStep
import re

WORD_REGEX = re.compile(r"[\w]+")

class MRMaxFreq(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(mapper=self.mapper_2,
                   reducer=self.reducer_2)
        ]

    def mapper(self, _, line):
        for word in WORD_REGEX.findall(line):
            yield word.lower(), 1

    def reducer(self, word, counts):
        yield word, sum(counts)


    # keys: None, values: (word, word_count)
    def mapper_post(self, word, word_count):
        yield None, (word, word_count)

    # sort list of (word, word_count) by word_count
    def reducer_post(self, _, word_count_pairs):
        yield max(word_count_pairs, key=lambda p: p[1],)

if __name__ == "__main__":
    MRMaxFreq().run()

```

We run it as previous, additionaly the output can be  redirected to the file "max_freq_word.txt"

``` sh
$ python word_freq_mrjob.py 'pg20701.txt' > 'max_freq_word.txt'
$ cat 'max_freq_word.txt'
"the" 14715
```

### Creating helper data structures

When we count the number of words, instead of yielding (word, 1) in mapper, we can initialize the mapper_init datastructure where we will store results, and mapper_final() which is used to post-proces data after main function. These methods can be used with all methods: mapper_init(), mapper_final(), reducer_init(), reducer_final(), and combiner_init(), combiner_final(). 

The modified word count example is

``` py title="mrjob_wc_1.py"
from collections import defaultdict
from mrjob.job import MRJob
import re

word_regex = re.compile(r"[\w]+")

class MRWordCount(MRJob):

    def mapper_init(self):
        self.words_dict = defaultdict(int)

    def mapper(self, _, line):
        for w in word_regex.findall(line):
            self.words_dict[word.lower()] += 1

    def mapper_final(self):
        for word, val in self.words_dict.items():
            yield word, val

    def reducer(self, word, counts):
        yield word, sum(counts)

```
Here, mapper_init() function create dictinary, the mapper populates the dictionary, while the mapper_final() preprocess the dictionary and yield (word, val) pairs. As mrjob runs in several processes we need reducer, which will combine partial counts made by each process. 

### Passing arguments to mrjob

Getting Serena Williams sisters rivaly with MRJob

``` py title="mrjob_williams.py"
from mrjob.job import MRJob
from functools import reduce

def make_counts(acc, nxt):
    acc[nxt] = acc.get(nxt,0) + 1
    return acc

def my_freq(xs):
    return reduce(make_counts, xs, {})

class Williams(MRJob):

    def mapper(self, _, line):
        fields = line.split(',')
        players = [fields[10], fields[18]] #(winner, loser)
        if 'Serena Williams' in players and 'Venus Williams' in players:
            yield fields[2], fields[10]

    def reducer(self, surface, results):
        counts = my_freq(results)
        yield surface, counts

if __name__ == "__main__":
    Williams.run()
```
``` sh 
python mrjob_wiliams.py tennis_wta-master/wta_matches_*
```

Instead of overcoding the script with 'Serena Williams' and Venus Williams we can pass arguments to mrjob using passargs option.  

``` py title="mrjob_2players.py"
from mrjob.job import MRJob
from functools import reduce

def make_counts(acc, nxt):
    acc[nxt] = acc.get(nxt,0) + 1
    return acc

def my_freq(xs):
    return reduce(make_counts, xs, {})

class Williams(MRJob):

    def configure_args(self):
        super(Williams, self).configure_args()
        self.add_passthru_arg("-p1", "--player1", help="player1")
        self.add_passthru_arg("-p2", "--player2", help="player1")


    def mapper(self, _, line):
        fields = line.split(',')
        players = [fields[10], fields[18]]
        if self.options.player1 in players and self.options.player2 in players:
            yield fields[2], fields[10]

    def reducer(self, surface, results):
        counts = my_freq(results)
        yield surface, counts

if __name__ == "__main__":
    Williams.run()
```

``` sh
python3 mrjob_2players.py tennis_wta-master/wta_matches_* --player1 "Serena Williams" --player2 "Venus Williams"
```
