
**Guojing Wu** | UNI: gw2383 | *2019-11-14*

# E6893 Big Data Analytics Homework4

## This is for question 3

Get the graphic data


```python
from graphframes import *  # for graph analysis
from pyspark import SparkConf, SparkContext
import pyspark
import sys

def getData(sc, filename):
    """
    Load data from raw text file into RDD and transform.
    Hint: transfromation you will use: map(<lambda function>).
    Args:
        sc (SparkContext): spark context.
        filename (string): hw2.txt cloud storage URI.
    Returns:
        RDD: RDD list of tuple of (<User>, [friend1, friend2, ... ]),
        each user and a list of user's friends
    """
    # read text file into RDD
    data = sc.textFile(filename).map(lambda line: line.split('\t'))

    # TODO: implement your logic here
    data = data.map(lambda tmp: (tmp[0], [num for num in tmp[1].split(',')]))

    return data

def getEdges(line):
    """
    get edges from input data
    
    Args:
        line (tuple): a tuple of (<User1>, [(<User2>, 0), (<User3>, 1)....])
    Returns:
        RDD of tuple (line[0], connected friend)
    """
    friends = line[1]
    for i in range(len(friends)):
        # Direct friend
        yield (line[0], friends[i])
        
conf = SparkConf()
sc = pyspark.SparkContext.getOrCreate(conf=conf)
sc.setCheckpointDir('/checkpoints')
# The directory for the file
filename = "gs://big_data_hw/hw2/q1.txt"

# Get data in proper format
data = getData(sc, filename)
vertices = data.map(lambda x: (x[0],))
edges = data.flatMap(getEdges)
V = spark.createDataFrame(vertices, ["id"])
E = spark.createDataFrame(edges, ["src", "dst"])
G = GraphFrame(V, E)
compo = G.connectedComponents()

# nodes
qNodes = compo.filter(compo["component"] == 103079215141).select('id')
qNodes.write.save('gs://big_data_hw/hw4/ndoes', format="json", mode="overwrite")

# edges 
tmp = [int(q.id) for q in qNodes.collect()]
# filter query text
qtext = "("
for i in range(len(tmp)-1):
    qtext += "src = {} or ".format(tmp[i])
qtext += "src = {}) and (".format(tmp[-1])
for i in range(len(tmp)-1):
    qtext += "dst = {} or ".format(tmp[i])
qtext += "dst = {})".format(tmp[-1])

qEdges = G.filterEdges(qtext).edges
x = qEdges.toPandas().replace([str(i) for i in tmp], [str(i) for i in range(len(tmp))])
qEdges = spark.createDataFrame(x)
qEdges.write.save('gs://big_data_hw/hw4/edges', format="json", mode="overwrite")
```


```python
import sys
import requests
import subprocess
from google.cloud import bigquery

# nodes
files = 'gs://big_data_hw/hw4/ndoes' + '/part-*'
subprocess.check_call(
    'bq load --source_format NEWLINE_DELIMITED_JSON '
    '--replace '
    '--autodetect '
    '{dataset}.{table} {files}'.format(
        dataset='my_dataset', table='nodes', files=files
    ).split())
output_path = sc._jvm.org.apache.hadoop.fs.Path('gs://big_data_hw/hw4/ndoes')
output_path.getFileSystem(sc._jsc.hadoopConfiguration()).delete(
    output_path, True)

# edges
files = 'gs://big_data_hw/hw4/edges' + '/part-*'
subprocess.check_call(
    'bq load --source_format NEWLINE_DELIMITED_JSON '
    '--replace '
    '--autodetect '
    '{dataset}.{table} {files}'.format(
        dataset='my_dataset', table='edges', files=files
    ).split())
output_path = sc._jvm.org.apache.hadoop.fs.Path('gs://big_data_hw/hw4/edges')
output_path.getFileSystem(sc._jsc.hadoopConfiguration()).delete(
    output_path, True)
```




    DataFrame[id: string]


