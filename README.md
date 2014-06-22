Synonym-Search-hadoop
=====================

Synonym Search Engine - Hadoop MapReduce, Apache Hive

+ Writing MapReduce functions to provide all the synonyms of a "Input Word" by the user.
+ Search Keyword is searched in Hive DB (works as Caching server), stored as Key,Value pair partitioned on Keyword.
+ Value returned if Cache Hit occurs.
+ If no match(Cache Miss), MapReduce is implemented by parsing through multiple dictionaries. The results are stored in Hive DB for further queries.
+ It is curerntly configured to be used on Single Node Hadoop installation.
