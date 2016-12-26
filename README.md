# OrpheusDB
[OrpheusDB][orpheus] is a hosted system that supports _relational dataset version management_. OrpheusDB is built on top of standard relational databases, thus it inherits much of the same benefits of relational databases, while also compactly storing, tracking, and recreating versions on demand, all very efficiently.

OrpheusDB is a multi-year project, supported by the National Science Foundation via award number 1513407. It shares the vision of the vision paper on the [DataHub][datahub] project in supporting collaborative data analytics.


<!-- OrpheusDB is an open-sourced database that enable data version capability on relational database system.This repository is an implementation of ongoing research under the project OrpheusDB at the University of Illinois at Urbana Champaign led by [Prof. Aditya Parameswaran][prof]. -->


### Version
The current version is 1.0.0 (Released January 1, 2017).

### Features
OrpheusDB is built using [PostgreSQL][postgressite] and [Click][clicksite], a command line tool written in Python. Our current version supports advanced querying capabilities, using both the git-style version control commands, as well as SQL queries on one or more dataset versions.

Our basic concept is that of a collaborative versioned dataset, or CVD --- a CVD is the basic unit of storage within OrpheusDB, representing a collection of versions of a single relational dataset. Users can operate on CVDs much like they would with source code version control. The _checkout_ command allows users to materialize one or more specific versions of a CVD as a newly created regular table within a relational database or as a csv file; the _commit_ command allows users to add a new version to a CVD by making the local changes made by the user on their materialized table or on their exported csv file visible to others. Other git-style commands we support include _init_, _create\_user_, _config_, _whoami_, _ls_, _db_, _drop_, and _optimize_.

Users can also execute SQL queries on one or more relational dataset versions within a CVD via the command line using the _run_ command, without requiring the corresponding dataset versions to be materialized. Beyond executing queries on a small number of versions, users can also apply aggregation grouped by version ids, or identify versions that satisfy some property. <!-- TODO: UPDATE/INSERT/REMOVE -->

### Key Design Innovations
* OrpheusDB is built on top of a traditional relational database, thus it inherits all of the standard benefits of relational database systems "for free"
* OrpheusDB supports advanced querying and versioning capabilities, via both SQL queries and git-style version control commands.
* OrpheusDB uses a sophisticated data model, coupled with partition optimization algorithms, to provide efficient version control performance over large-scale datasets. 


### System Requirement
OrpheusDB requires the following software to be installed successfully prior to setup: 
* Python 2.7.x
* PostgreSQL >= 9.5

### Installation Instructions
OrpheusDB comes with a standard `setup.py` script for installation. The required python dependency packages include
* click >= 6.6
* psycopy2 >= 2.6.2
* pandas >= 0.19.0
* pyyaml >= 3.12
* pyparsing >=2.1.1

Users are able to install any of missing dependencies themselves via `pip`. Alternatively, an easier way to install all the requisite dependencies is via `pip install .`

After installation, users can use `dh --help` to list all the available commands in OrpheusDB. By default, `dh` is the alias for OrpheusDB user interface.

<!--
```
pip install .
dh --help
```
-->

### Configuration
OrpheusDB needs to know where the underlying relational database storage is located before execution. To specify the associated parameters, change the corresponding fields in `config.yaml`.

### Dataset Version Control in OrpheusDB
The fundamental unit of storage within OrpheusDB is a collaborative versioned dataset (CVD) to which one or more users can contribute. Each CVD corresponds to a relation with a fixed schema, and implicitly contains many versions of that relation. There is a many-to-many relationship between records in the relation and versions that is captured within the CVD: each record can belong to many versions, and each version can contain many records. Each version has a unique version id integer, namely vid.
<!-- Collaborative Version Dataset is the unit of operation in OrpheusDB. Each CVD stores dataset and its version information. Each version is represented with an unique version vid, _vid_. -->

### User Tutorials
To start with, user can create an OrpheusDB username with a password via the `create_user` command. Upon finishing, it will be pushed to the underlying data storage with a SUPERUSER privilege. Command `config` is used to login through created user and `whoami` is used to list the current user name that is currently logged in. 

Please note here that OrpheusDB provides the most basic implementation for user information, i.e. there is no password protection. However, this feature is subject to change in future versions.
```
dh create_user
dh config
dh whoami
```

The `init` command provides ways to load a csv file into OrpheusDB as a CVD, with the all records as its first version (i.e., vid = 1). To let OrpheusDB know what is the schema for this dataset, user can provide a sample schema file through option `-s`. Each line in the schema file has the format `<attribute name>, <type of the attribute>`. In the following example, `data.csv` file contains 3 attributes, namely `age`, `employee_id` and `salary`. The command below loads the `data.csv` file into OrpheusDB as a CVD named `dataset1` whose schema is indicated in the ``sample_schema.csv`. 

<!-- In the current release, only `csv` file format is supported in the `init`. -->

```
dh init data.csv dataset1 -s sample_schema.csv
```

User can checkout one or more desired versions through the `checkout` command, to either a csv file or a structured table in RDMS. <!-- Again, only `csv` format is supported. --> In the following example, it checkouts the version 1 of CVD dataset1 as a csv file named checkout.csv. 
```
dh checkout dataset1 -v 1 -f checkout.csv
```
Any changed or new records from commit file will be appended to the corresponding CVD, labeled with a new version id. One special case is the committing of a subset of previous checkedout version. For such case, OrpheusDB will commit as user wishes.

After changes are made to the previous checkout versions, OrpheusDB can commit these changes to its corresponding CVD assuming unchanged schema. 

In the following example, it commits the modified checkout.csv back to CVD dataset1. Note here since OrpheusDB internally logged the CVD that checkout.csv was checked out from, there is no need to specify the CVD name in the `commit` command. 
```
dh commit -f checkout.csv -m 'first commit'
```

To avoid the cost of additional storage, OrpheusDB also supports query against CVD. The run command will prompt user with input to execute SQL command directly. If `-f` is specified, it will execute the SQL file specified.  
```
dh run
```

OrpheusDB supports a rich syntax of SQL statements. During the execution, OrpheusDB will detect keywords like `CVD` so it knows the query is against CVD. In the following example, OrpheusDB will select the `age` column from CVD dataset1 whose version id is equal to either `1` or `2`.
```
SELECT age FROM VERSION 1,2 OF CVD dataset1;
```

If version number is unknown, OrpheusDB also supports query against it. The follow statement will select those version numbers that any records reside in match the conditions listed in the where constraint. It is worth noticing that the `GROUP BY` clause is required to aggregate on versions.

In the following example, OrpheusDB will select all the version ids that have one or more records whose age equals to 25.
```
SELECT vid FROM CVD dataset1 WHERE age = 25 GROUP BY vid;
```
A few more SQL examples are:

(1). Find all versions in CVD `dataset1` that have more than 100 records whose salary is larger than 7400.
```
SELECT vid FROM CVD dataset1 WHERE salary > 7400 GROUP BY vid HAVING COUNT(employee_id) > 100;
```
(2). Find all versions in CVD `dataset1` whose commit time is later than December 1st, 2017.
```
SELECT vid FROM CVD dataset1 WHERE commit_time >  '2017-12-01';
```



### Todos
 - ~~db run~~
 - ~~change user password settings~~
 - tracker overwrite, get rid of the old mapping
 - ~~change cvd to public schema~~ Not work for INFORMATION_SCHEMA.COLUMNS 
 - update meta after dropping dataset
 - update load current state path from .meta/config
 - $ORPHEUS_HOME$ in bashrc
 - verbose mode
 - mock testing
 
License
----

MIT

[//]: # (These are reference links used in the body of this note and get stripped out when the markdown processor does its job. There is no need to format nicely because it shouldn't be seen. Thanks SO - http://stackoverflow.com/questions/4823468/store-comments-in-markdown-syntax)

   [prof]: http://web.engr.illinois.edu/~adityagp/#
   [clicksite]: http://click.pocoo.org/5/
   [orpheus]: http://orpheus-db.github.io/
   [datahub]: https://arxiv.org/abs/1409.0798
   [postgressite]: https://www.postgresql.org/
