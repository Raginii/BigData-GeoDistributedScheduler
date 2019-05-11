# BigData-GeoDistributed-Scheduler

This repo contains changes made atop Hadoop- 2.9 (initial version of YARN Federation) customized to a geo-distributed setup. 

The config_files folder provides an example of how geo-distributed partitions are created in a multi-site region and how they are setup. The example join folder provides with test join queries used to debug the federation architecture. 

Hive benchmark folder provides the components required to execute different kinds of queries (Scan, aggregate, join) on the federated setup inspired by amplab.
