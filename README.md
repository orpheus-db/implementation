# OrpheusDB

OrpheusDB is an open-sourced database that enable data version capability on relational database system.

This repository is an implementation of ongoing research under the project OrpheusDB at the University of Illinois at Urbana Champaign led by [Prof. Aditya Parameswaran][prof].

### Version
1.0.0

### System Requirement
* Python 2.7.x
* PostgreSQL >= 9.5

### Install
OrpheusDB comes with standard `setup.py` script for installation. An easier way to install is through pip. By default, `dh` is the alias for OrpheusDB user interface.

```
pip install .
dh --help
```

### Configuration
OrpheusDB needs to know where is the underlying relational database storage before execution. To specify such, change the corresponding fields in `config.yaml`.

### Collaborative Version Dataset (CVD)
Collaborative Version Dataset is the unit of operation in OrpheusDB. Each CVD stores dataset and its version information.

### Tutorials
Create a OrpheusDB user named `abc`. Upon finishing, it will be pushed to the underlying data storage with *SUPERUSER* privilege. Command `config` is to login through created user.
```
dh create_user
dh config
dh whoami
```
OrpheusDB provides the most basic implementation for user information, i.e. there is no password protection. However, this feature is subject to change in future version.

The `init` command provides ways to load file into OrpheusDB (CVD), with the all records as the first version. To let OrpheusDB know what is the schema for this dataset, user can provide a sample schema file through option `-s`. In the current release, only `csv` file format is supported. In this example, `data.csv` file contains 3 attributes, namely `age`, `employee_id' and 'salary'.
```
dh init data.csv dataset1 -s sample_schema.csv
```

User can checkout desired version from the `checkout` command, to either a file or a table. Again, only `csv` format is supported.
```
dh checkout dataset1 -v 1 -f checkout.csv
```

After changes are made to the previous checkout versions, OrpheusDB can commit these changes to its corresponding CVD assuming unchanged schema.
```
dh commit -f checkout.csv -m 'first commit'
```
Any changed or new records from commit file will be appended to the corresponding CVD, labaled with a new version. One special case is the committing of a subset of previous checkedout version. For such case, OrpheusDB will commit as user wishes.

To avoid the cost of additional storage, OrpheusDB also supports query against CVD. The run command will prompt user with input to execute SQL command directly. If `-f` is specified, it will execute the SQL file specified.  
```
dh run
```

OrpheusDB supports a richer syntax of SQL statements. During the execution, OrpheusDB will detect keywords like `CVD` so it knows the query is against CVD. This statement will select the `age` column from version `1` and `2`.
```
SELECT age FROM VERSION 1,2 OF CVD dataset1
```

If version number is unknown, OrpheusDB also supports query against it. The follow statement will select those version numbers that any records reside in match the where constraint. It is worth noticing that the `GROUP BY` clause is required to aggreate on version.
```
SELECT vid FROM CVD ds1 WHERE age = 25 GROUP BY vid
```

### Todos
 - db run
 - ~~change user password settings~~
 - tracker overwrite, get rid of the old mapping
 - change cvd to public schema
 - update meta after dropping dataset
 - update load current state path from .meta/config
 
License
----

MIT

[//]: # (These are reference links used in the body of this note and get stripped out when the markdown processor does its job. There is no need to format nicely because it shouldn't be seen. Thanks SO - http://stackoverflow.com/questions/4823468/store-comments-in-markdown-syntax)

   [prof]: http://web.engr.illinois.edu/~adityagp/#
