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

### Commands
TODO

### Todos
 - db run
 - change user password settings. 
 - tracker overwrite, get rid of the old mapping
 - change cvd to public schema
 - update meta after dropping dataset
 - update load current state path from .meta/config
 
License
----

MIT

[//]: # (These are reference links used in the body of this note and get stripped out when the markdown processor does its job. There is no need to format nicely because it shouldn't be seen. Thanks SO - http://stackoverflow.com/questions/4823468/store-comments-in-markdown-syntax)

   [prof]: http://web.engr.illinois.edu/~adityagp/#
