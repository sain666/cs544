# P4 (5% of grade): HDFS Partitioning and Replication

## Overview

HDFS can *partition* large files into blocks to share the storage
across many workers, and it can *replicate* those blocks so that data
is not lost even if some workers die.

In this project, you'll deploy a small HDFS cluster and upload a large
file to it, with different replication settings.  You'll write Python
code to read the file.  When data is partially lost (due to a node
failing), your code will recover as much data as possible from the
damaged file.

**Remember to switch to an e2-medium for this project:** [VM schedule](../projects.md#compute-setup). Note you can edit your existing instance to an e2-medium instead of deleting your old and creating a new vm. This [tutorial](https://cloud.google.com/compute/docs/instances/changing-machine-type-of-stopped-instance) should help you switch over.

If you instead start on a new machine, remember to reinstall Docker
and also to enable Docker to be run without sudo. Refer to P1 for
instructions.

Learning objectives:
* use the HDFS command line client to upload files
* use the webhdfs API (https://hadoop.apache.org/docs/r3.3.6/hadoop-project-dist/hadoop-hdfs/WebHDFS.html) to read files
* use PyArrow to read HDFS files
* relate replication count to space efficiency and fault tolerance

Before starting, please review the [general project directions](../projects.md).

## Corrections/Clarifications

* 17 Oct, 2024: `autograde.py` and `check_sub.py` published.
* 17 Oct, 2024: Details regarding output updated (the third point of [Grading](#grading)).
* 18 Oct, 2024: Correction of description in [Q8](#q8-how-much-faster-can-we-make-the-previous-code-if-we-only-read-the-loan_amount-column)
, the answer should be time taken in Q7 divided by time taken in Q8.
* 22 Oct, 2024: Expected range of answer for [Q8](#q8-how-much-faster-can-we-make-the-previous-code-if-we-only-read-the-loan_amount-column) relaxed from \[10,40\] to \[7,60\], and a note  regarding grading details also attached.
* 24 Oct, 2024: Autograder fixed to avoid starting tests before HDFS detects that there are missing blocks for [Q10](#q10-how-many-blocks-of-singleparquet-were-lost).

## Part 1: Deployment and Data Upload

Before you begin, please run the below command in your p4 directory. This will stop git from trying to track csv files which will save you a lot of headaches (This step will not be graded - it is just to help you).

```
echo "*.csv" >> .gitignore
echo "*.parquet" >> .gitignore
```
#### Cluster

For this project, you'll deploy a small cluster of containers, one
with Jupyter, one with an HDFS NameNode, and two with HDFS DataNodes.

We have given you `docker-compose.yml` for starting the cluster, but
you need to build some images first.  Start with the following:

```
docker build . -f hdfs.Dockerfile -t p4-hdfs
docker build . -f notebook.Dockerfile -t p4-nb
```

The second image depends on the first one (`p4-hdfs`) allowing us to avoid repeating imports --you can see
this by checking the `FROM` line in "notebook.Dockerfile".

The compose file also needs `p4-nn` (NameNode) and `p4-dn` (DataNode)
images.  Create Dockerfiles for these that can be built like this:

```
docker build . -f namenode.Dockerfile -t p4-nn
docker build . -f datanode.Dockerfile -t p4-dn
```

Requirements:
* like `p4-nb`, both these should use `p4-hdfs` as a base
* `namenode.Dockerfile` should run two commands, `hdfs namenode -format` and `hdfs namenode -D dfs.namenode.stale.datanode.interval=10000 -D dfs.namenode.heartbeat.recheck-interval=30000 -fs hdfs://boss:9000`
* `datanode.Dockerfile` should just run `hdfs datanode -D dfs.datanode.data.dir=/var/datanode -fs hdfs://boss:9000`

You can use `docker compose up -d` to start your mini cluster.  You
can run `docker compose kill; docker compose rm -f` to stop and delete
all the containers in your cluster as needed.  For simplicity, we
recommend this rather than restarting a single container when you need
to change something as it avoids some tricky issues with HDFS.  For
example, if you just restart+reformat the container with the NameNode,
the old DataNodes will not work with the new NameNode without a more
complicated process/config.


#### Data Upload

Connect to JupyterLab running in the `p4-nb` container, and create a
notebook called `p4a.ipynb` in the "/nb" directory (which is shared
with the VM).  We'll do some later work in another notebook,
`p4b.ipynb`.

#### Q1: how many live DataNodes are in the cluster?

Write a cell like this (and use a similar format for other questions):

```
#q1
! SHELL COMMAND TO CHECK
```

The shell command should generate a report by passing some arguments
to `hdfs dfsadmin`.  The output should contain a line like this (you
might need to wait and re-run this command a bit to give the DataNodes
time to show up):

```
...
Live datanodes (2):
...
```

Write some code (Python or shell) that downloads
https://pages.cs.wisc.edu/~harter/cs544/data/hdma-wi-2021.csv if it
hasn't already been downloaded.

Next, transform this `CSV` file into a `Parquet` file using pyarrow.

Then, use two `hdfs dfs -cp` commands to upload this same **parquet file**, instead of that CSV file to HDFS
twice, to the following locations:

* `hdfs://boss:9000/single.parquet`
* `hdfs://boss:9000/double.parquet`

In both cases, use a 1MB block size (`dfs.block.size`), and
replication (`dfs.replication`) of 1 and 2 for `single.parquet` and
`double.parquet`, respectively.

If you want to re-run your notebook from the top and have the files
re-created, consider having a cell with the following, prior to the
`cp` commands.

```
!hdfs dfs -rm -f hdfs://boss:9000/single.parquet
!hdfs dfs -rm -f hdfs://boss:9000/double.parquet
```

#### Q2: what are the logical and physical sizes of the parquet files?

Run a `du` command with `hdfs dfs` to see.

You should see something like this:

```
15.9 M  31.7 M  hdfs://boss:9000/double.parquet
15.9 M  15.9 M  hdfs://boss:9000/single.parquet
```

The first columns show the logical and physical sizes.  The two parquet files
contain the same data, so the have the same logical sizes.  Note the
difference in physical size due to replication, though.

## Part 2: WebHDFS

The documents here describe how we can interact with HDFS via web requests: https://hadoop.apache.org/docs/r3.3.6/hadoop-project-dist/hadoop-hdfs/WebHDFS.html.

Many examples show these web requests being made with the `curl` command, but you'll adapt those examples to use `requests.get` (https://requests.readthedocs.io/en/latest/user/quickstart/).

By default, WebHDFS runs on port 9870. **So use port 9870 instead of 9000 for this part.**

#### Q3: what is the file status for single.parquet?

Use the `GETFILESTATUS` operation to find out, and answer with a
dictionary (the request returns JSON).

Note that if `r` is a response object, then `r.content` will contain
some bytes, which you could convert to a dictionary; alternatively,
`r.json()` does this for you.

The result should look something like this:

```
{'FileStatus': {...
  'blockSize': 1048576,
  ...
  'length': 16642976,
  ...
  'replication': 1,
  'storagePolicy': 0,
  'type': 'FILE'}}
```

The `blockSize` and `length` fields might be helpful for future
questions.

#### Q4: what is the location for the first block of single.parquet?

Use the `OPEN` operation with `offset` 0 and `noredirect=true` - (`length` and `buffersize` are optional).
You answer should a string, similar to this:

```python
'http://b5037853ed0a:9864/webhdfs/v1/single.parquet?op=OPEN&namenoderpcaddress=boss:9000&offset=0'
```

Note that `b5037853ed0a` was the randomly generated container ID for
the container running the DataNode, so yours will be different.

#### Q5: how are the blocks of single.parquet distributed across the two DataNode containers?

This is similar to above, except you should check every block and extract the container ID from the URL.

You should produce a Python dictionary similar to below (your IDs and counts will be different, of course).

```python
{'755329887c2a': 9, 'c181cd6fd6fe': 7}
```

If all the blocks are on the same DataNode, it is likely you uploaded
the Parquet data before both DataNodes got a chance to connect with
the NameNode.  Re-run, this time giving the cluster more time to come
up.

#### Q6: how are the blocks of double.parquet distributed across the two DataNode containers?

This looks similar to above, but if you try to use a same approach,
you may find something strange, like the distribution of the blocks is
not fixed. Sometimes the result might be:

```python
{'c181cd6fd6fe': 11, '755329887c2a': 5}
```
and sometimes it's like:
```python
{'c181cd6fd6fe': 9, '755329887c2a': 7}
```
or change to:
```python
{'755329887c2a': 7, 'c181cd6fd6fe': 9}
```

Take a while to think about the reasons for this behavior.

To get the accurate distribution of the datablocks, you can use `GETFILEBLOCKLOCATIONS` operation.

The return of `GETFILEBLOCKLOCATIONS` operation is also a response object. If `r` is the return response object, then `r.json()` should look like this:

```python
{'BlockLocations': {'BlockLocation': [
    {
      'topologyPaths': ['/default-rack/172.18.0.6:9866',
      '/default-rack/172.18.0.5:9866'],
      ...
      'hosts': ['c3e203ed7f82', '048c8af1c83d'],
      ...
    },
    {
      'topologyPaths': ['/default-rack/172.18.0.7:9866',
      '/default-rack/172.18.0.4:9866'],
      ...
      'hosts': ['70f346e5e56f', '3a7e0e314be0'],
      ...
    },
    ...
   ]}}
```

To answer this question, you should produce a Python dictionary, and the answer should also similar to below (your IDs will be different, of course).


```python
{'c181cd6fd6fe': 16, '755329887c2a': 16}
```

## Part 3: PyArrow

#### Q7: What is the average `loan_amount` in the double.parquet file?

Use PyArrow to read the HFDS file.  You can connect to HDFS like this (the missing values are host and port, respectively):

Hint: Think about which port we should connect on.

```python
import pyarrow as pa
import pyarrow.fs
hdfs = pa.fs.HadoopFileSystem(????, ????)
```

You can then use `hdfs.open_input_file(????)` with a path to open the
file and return a `pyarrow.lib.NativeFile` object.  You can then read
the table from f, as in lecture:
https://git.doit.wisc.edu/cdis/cs/courses/cs544/f24/main/-/blob/main/lec/13-file-systems/lec1/demo.ipynb

In addition to the answer, your code should measure how long the
operation takes (including both time to read the file and do the mean)
this measurement will help with the next question.  Do not optimize
the code by only reading a subset of columns (read all the columns,
which is the default).

#### Q8: How much faster can we make the previous code if we only read the `loan_amount` column?

Copy and modify the code from Q7 to implement this optimization, and
again record the time.  The answer should be a multiple (time taken in
Q7 divided by time taken in Q8).  Performance varies a lot, but the
optimized version will probably be 7-60x faster.

**Note:** Sometimes the speedup ratio might fluctuate and be out of expected range. When grading, in that case, TA will check the code manully. Ensure your code just read the single column of `loan_amount`. 

## Part 4: Disaster Strikes

Do the following:
* manually kill one of the DataNode containers with a `docker kill` command
* start a new notebook in Jupyter called `p4b.ipynb` -- use it for the remainder of your work

#### Q9: how many live DataNodes are in the cluster?

This is the same question as Q1, but now there should only be one:

```
...
Live datanodes (1)
...
```

You might need to wait a couple minutes and re-run this until the NameNode recognizes that the DataNode has died.

#### Q10: how many blocks of single.parquet were lost?

There are a couple strategies you could use to find the answer:
* `OPEN` call: you'll get a different HTTP status code at offsets corresponding to lost blocks)
* `GETFILEBLOCKLOCATIONS` call: there will be no DataNode "hosts" for lost blocks

## Grading

* Copy the `autograde.py` file to your working directory, then execute the command `python3 autograde.py` to test your work. After running the script, your score will be saved in the `score.json` file.

* For debugging, you may need to check the outputs of your notebooks from the autograder. After running the autograder, an `_autograder_nb` directory will be created, where you can find the executed notebooks.

* Make sure your answers are in cell output - not print statements (see the example below)
  ```
  my_answer = []
  for i in range(5):
    my_answer.append(5)
  my_answer
  ```

## Submission

We should be able to run the following on your submission to create the mini cluster:

```
docker build . -f hdfs.Dockerfile -t p4-hdfs
docker build . -f namenode.Dockerfile -t p4-nn
docker build . -f datanode.Dockerfile -t p4-dn
docker build . -f notebook.Dockerfile -t p4-nb
docker compose up -d
```

We should then be able to open `http://localhost:5000/lab` and find
your `p4a.ipynb` and `p4b.ipynb` notebooks and run them.

To make sure you didn't forget to push anything, we recommend doing a
`git clone` of your repo to a new location and going through these
steps as a last check on your submission.

You're free to create and include other code files if you like (for
example, you could write a .py module used by both notebooks).

After pushing your code to your designated GitLab repository, you can verify your submission by copying `check_sub.py` to your working directory and running the command `python3 check_sub.py`.

<!--
## Tester:(To be updated)
* Expected that you use Python 3.10.12
* Run `setup.sh` to install the packages needed for the autograder (you may already have them installed - just in case)
* After you push your final submission, try cloning your repo into a new temp folder and run the test there; this will simulate how we run the tests during grading. 
* Copy in `tester.py` from the main github directory into your p4 folder
* You can run `python3 autograde.py -g` to create a debug directory which will contain the notebooks that were used for testing. This will let you examine the state of the notebooks and catch errors
* Make sure your answers are in cell output - not print statements (see the example below)
```
my_answer = []
for i in range(5):
  my_answer.append(5)
my_answer
```
-->