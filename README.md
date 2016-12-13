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
OrpheusDB provides the most basic implementation for user information, subject to change in later version.

The `init` command provides ways to load file into OrpheusDB as CVD as the first version. To let OrpheusDB know what is the schema for this dataset, user can provide a sample schema file through option `-s`. In the current release, only `csv` file format is supported.
```
dh init data.csv dataset1 -s sample_schema.csv
```

User can checkout desired version from he `checkout` command, to either a file or a table. Again, only `csv` format is supported.
```
dh checkout dataset1 -v 1 -f checkout.csv
```

After changes are made to checkout versions, OrpheusDB can commit these changes to its corresponding CVD assuming the schema remains the same.
```
dh commit -f checkout.csv -m 'first commit'
```

To avoid the cost of additional storage, OrpheusDB also supports query against CVD. The run command will prompt user with input to execute SQL command directly. If `-f` is specified, it will execute the SQL file specified.  
```
dh run
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
