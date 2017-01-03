# OrpheusDB: Bolt-On Versioning for Relational Databases
[OrpheusDB][orpheus] is a hosted system that supports _relational dataset version management_. OrpheusDB is built on top of standard relational databases, thus it inherits much of the same benefits of relational databases, while also compactly storing, tracking, and recreating versions on demand, all very efficiently.

OrpheusDB is built using [PostgreSQL][postgressite] and [Click][clicksite], a command line tool written in Python. Our current version supports advanced querying capabilities, using both the git-style version control commands, as well as SQL queries on one or more dataset versions. The paper describing the design, functionality, optimization, and performance evaluation can be found [at this link][papersite].

OrpheusDB is a multi-year project, supported by the National Science Foundation via award number 1513407. It shares the vision of the [DataHub][datahub] project in supporting collaborative data analytics.


<!-- OrpheusDB is an open-sourced database that enable data version capability on relational database system.This repository is an implementation of ongoing research under the project OrpheusDB at the University of Illinois at Urbana Champaign led by [Prof. Aditya Parameswaran][prof]. -->


### Version
The current version is 1.0.0 (Released January 1, 2017).


### Key Design Innovations
* OrpheusDB is built on top of a traditional relational database, thus it inherits all of the standard benefits of relational database systems "for free"
* OrpheusDB supports advanced querying and versioning capabilities, via both SQL queries and git-style version control commands.
* OrpheusDB uses a sophisticated data model, coupled with partition optimization algorithms<sup>1</sup>, to provide efficient version control performance over large-scale datasets. 

### Dataset Version Control in OrpheusDB
The fundamental unit of storage within OrpheusDB is a _collaborative versioned dataset (CVD)_ to which one or more users can contribute, 
representing a collection of versions of a single relational dataset, with a fixed schema. There is a many-to-many relationship between records in the relation and versions that is captured within the CVD: each record can belong to many versions, and each version can contain many records. <!--Each version of the CVDhas a unique version id integer, namely vid.-->
<!-- Collaborative Version Dataset is the unit of operation in OrpheusDB. Each CVD stores dataset and its version information. Each version is represented with an unique version vid, _vid_. --> 

Users can operate on CVDs much like they would with source code version control. The _checkout_ command allows users to materialize one or more specific versions of a CVD as a newly created regular table within a relational database or as a csv file; the _commit_ command allows users to add a new version to a CVD by making the local changes made by the user on their materialized table or on their exported csv file visible to others. Other git-style commands we support include _init_, _create\_user_, _config_, _whoami_, _ls_, _drop_, and _optimize_.

Users can also execute SQL queries on one or more relational dataset versions within a CVD via the command line using the _run_ command, without requiring the corresponding dataset versions to be materialized. Beyond executing queries on a small number of versions, users can also apply aggregation grouped by version ids, or identify versions that satisfy some property. <!-- TODO: UPDATE/INSERT/REMOVE -->



### Data Model
Each CVD in OrpheusDB corresponds to three underlying relational tables: the _data_ table, the _index_ table, and the _version_ table. To capture dataset versions, we represent the records of a dataset in the _data_ table and mapping between versions and records in the _index_ table. Finally, we store version-level provenance information in the _version_ table, including attributes such as `author`, `num_records`,  `parent`, `children`, `create_time`, `commit_time`, and `commit_msg`.

<!-- Our experimental evaluation demonstrates that, comparing to other alternatively data models, our data model paired with the partition optimizer has about `10x` less storage consumption, `1000x` faster for the commit operation.  -->

Our experimental evaluation demonstrates that, compared to other alternative data models, our data model, coupled with the partition optimizer results in **10x** less storage consumption, **1000x** less time for _commit_ and comparable query performance for the _checkout_ command. In other words, OrpheusDB acheives an efficient balance between storage consumption and query latencies.

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
* sqlparse >= 0.2.2

Users are able to install any of missing dependencies themselves via `pip`. Alternatively, an easier way to install all the requisite dependencies is via `pip install .` (If you encounter permission errors, install via `sudo -H pip install .`)

After installation, users can use `dh --help` to list all the available commands in OrpheusDB. By default, `dh` is the alias for OrpheusDB user interface.

<!--
```
pip install .
dh --help
```
-->

### Configuration
Users need to install PostgresSQL successfully (A tutorial of installing PostgresSQL on Mac OSX can be found [here][postgres-installation].) After starting PostgresSQL(e.g., via pg_ctl), users can call `createdb` to set up a new database with a new username and password. 

OrpheusDB needs to know where the underlying relational database-based storage engine is located before execution. To specify the associated parameters, change the corresponding fields in `config.yaml`. Moreover, users have to connect OrpheusDB to an existing PostgresSQL database and a valid user name via command `dh config`. 

### User Tutorials
User can create a new OrpheusDB username with a password via the `create_user` command. Upon finishing, it will be pushed to the underlying data storage with a SUPERUSER privilege. Command `config` can also be used to login through created user and `whoami` is used to list the current user name that is currently logged in. 

Please note here that OrpheusDB provides the most basic implementation for user information, i.e. there is no password protection. However, this feature is subject to change in future versions.
```
db config
dh create_user
dh whoami
```

The `init` command provides ways to load a csv file into OrpheusDB as a CVD, with the all records as its first version (i.e., vid = 1). To let OrpheusDB know what is the schema for this dataset, user can provide a sample schema file through option `-s`. Each line in the schema file has the format `<attribute name>, <type of the attribute>`. In the following example, `data.csv` file contains 3 attributes, namely `age`, `employee_id` and `salary`. The command below loads the `data.csv` file under the same directory into OrpheusDB as a CVD named `dataset1`, whose schema is indicated in the file ``sample_schema.csv`. 

<!-- In the current release, only `csv` file format is supported in the `init`. -->

```
dh init test/data.csv dataset1 -s test/sample_schema.csv
```

User can checkout one or more desired versions through the `checkout` command, to either a csv file or a structured table in RDBMS. <!-- Again, only `csv` format is supported. --> In the following example, version 1 of CVD dataset1 is checked out as a csv file named checkout.csv. 
```
dh checkout dataset1 -v 1 -f checkout.csv
```

After changes are made to the previous checkout versions, OrpheusDB can commit these changes to its corresponding CVD assuming that the schema is unchanged. 

In the following example, we commit the modified checkout.csv back to CVD dataset1. Note here that since OrpheusDB internally logged the CVD that checkout.csv was checked out from, there is no need to specify the CVD name in the `commit` command. 

Any changed or new records from commit file will be appended to the corresponding CVD, labeled with a new version id. A special case is the committing of a subset of previously checked-out version. In such a setting, OrpheusDB will perform the commit as expected; the new version is added with the subset of the records.

```
dh commit -f checkout.csv -m 'first commit'
```

OrpheusDB also supports direct execution of queries on CVDs without materialization. This is done via the run command. The run command will prompt the user to provide the SQL command to be executed directly. If `-f` is specified, it will execute the SQL file specified.  
```
dh run
```

OrpheusDB supports a rich syntax of SQL statements on versions and CVDs. During the execution of these steatements, OrpheusDB will detect keywords like `CVD` so it knows the query is against one or more CVDs. There are mainly the following two types of queries supported.

1. Query against known version(s) of a particular dataset
2. Query against unknown version(s) of a particular dataset

To query against known version(s), the version number needs to be specified. In the following example, OrpheusDB will select the `employee_id` and  `age` columns from CVD dataset1 whose version id is equal to either `1` or `2`.
```
SELECT employee_id, age FROM VERSION 1,2 OF CVD dataset1;
```

If version number is unknown, OrpheusDB supports queries where the desired version number is also identified. In the following examples, OrpheusDB will select all the version ids that have one or more records whose age is less than 25. It is worth noting that the `GROUP BY` clause is required to aggregate on version number.
```
SELECT vid FROM CVD dataset1 WHERE age < 25 GROUP BY vid;
```
Here are a couple other examples of SQL on versions:

(1). Find all versions in CVD `dataset1` that have more than 5 records where salary is larger than 7400.
```
SELECT vid FROM CVD dataset1 WHERE salary > 7400 GROUP BY vid HAVING COUNT(employee_id) > 5;
```
(2). Find all versions in CVD `dataset1` whose commit time is later than December 1st, 2016.
```
SELECT vid FROM CVD dataset1 WHERE commit_time >  '2016-12-01' GROUP BY vid;
```

### Development Plan
We plan to release versions of OrpheusDB in a regular manner, adding on further
querying, partitioning, and query optimization capabilities, as well as regular bug-fixes.
The known bugs are listed below.

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
   [papersite]:http://data-people.cs.illinois.edu/papers/orpheus.pdf
   [postgres-installation]: https://chartio.com/resources/tutorials/how-to-start-postgresql-server-on-mac-os-x/
   <sup>1</sup>The partition optimization algorithms are not part of this release.
