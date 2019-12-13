# Frequent itemset mining
### Project CS6240
### Fall 2019

Code authors
-----------

Akhil Nair, Shane Timmerman, Yexin Wang

---

Installation
------------
These components are installed:
- JDK 1.8
- Hadoop 2.9.1
- Maven
- AWS CLI (for EMR execution)

---

Environment
-----------
1) Example `~/.bash_aliases`:
```
export JAVA_HOME=/usr/lib/jvm/java-8-oracle
export HADOOP_HOME=/home/joe/tools/hadoop/hadoop-2.9.1
export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
```

2) Explicitly set `JAVA_HOME` in `$HADOOP_HOME/etc/hadoop/hadoop-env.sh`:
```
export JAVA_HOME=/usr/lib/jvm/java-8-oracle
```

---

Data
---------

The data we are interrogating is the 2009 Netflix Prize dataset, containing user views for movies, to identify ratings habits for consumers. The data must be downloaded from https://www.kaggle.com/netflix-inc/netflix-prize-data
We are using the `combination_data_{x}.txt` files, and the `qualifying.txt` files (since we are not training a machine learning model, this can be used for our data mining). Download the files and place them into the `/inputs` folder. Before using the data, run `python3 ./scripts/data_format.py`. This script simply add another new line between movies, allowing us to more easily define a delimiter before our initial map.

---

Execution
---------
All of the build & execution commands are organized in the `Makefile`.
1) Unzip project file.
2) Open command prompt.
3) Navigate to directory where project files unzipped.
4) Edit the `Makefile` to customize the environment at the top.
	- Sufficient for standalone: `hadoop.root`, `jar.name`, `local.input`
	- Other defaults acceptable for running standalone.
5) Standalone Hadoop:
```
	make switch-standalone		-- set standalone Hadoop environment (execute once)
	make local
```
6) Pseudo-Distributed Hadoop: (https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Pseudo-Distributed_Operation)
```
	make switch-pseudo			-- set pseudo-clustered Hadoop environment (execute once)
	make pseudo					-- first execution
	make pseudoq				-- later executions since namenode and datanode already running 
```
7) AWS EMR Hadoop: (you must configure the `emr.*` config parameters at top of `Makefile`)
```
	make upload-input-aws		-- only before first execution
	make aws					-- check for successful execution with web interface (aws.amazon.com)
	download-output-aws			-- after successful execution & termination
```

Report
---------
[Project Final Report](https://docs.google.com/document/d/1bCRV1WDyo_LZvj75WZkISbsBSSFPTUl8E1f9xW_pG8I/edit?usp=sharing)


