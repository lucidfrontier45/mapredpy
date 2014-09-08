mapred.py
===
This is a one-file python MapReduce template.
You can use this for prototyping your hadoop job.

How to use?
---
Subclass `_BaseMapper` and `_BaseReducer` and implement their `map` and `reduce` methods.
Optionally, you can add some initialization task to `__init__` of your Mapper and Reducer Class.

When run in local for debug to show in stdout

`cat <file> | ./mapred.py -m mapper | sort | ./mapred.py -m reducer`

When run in Hadoop cluster to output in HDFS

`hadoop jar <path_to_hadoop_streaming-jar> --files mapred.py -mapper './mapred.py -m mapper' -reducer './mapred.py -m reducer' -input <input in HDFS> -output <output in HDFS>`

Note
---
Using pypy will give you significant performance gain. Just change the first line of mapred.py.
You also need to install pypy in all the machines of you cluster.
