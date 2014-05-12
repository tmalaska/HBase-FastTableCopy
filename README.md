HBase-FastTableCopy
-----------------------------

### Problem
I have two tables.  One with table 'A' with 200 regions and table 'B' with 800 regions.  I want to copy data from table 'A' to table 'B'.

This implementation will scan table 'A' and generated the HFiles for table 'B' then use the HBase import command to copy the files into the HBase table.

### How to Run

* *Set up sample tables*
hadoop jar SmallToLargeTableCopy.jar CreateTables small large c 20 80

* *Populate the small table*
hadoop jar SmallToLargeTableCopy.jar PopulateSmallTable 10 100000 hbase/tmp small c 1

* *Copy the data from the small table and generate new HFiles*
hadoop jar SmallToLargeTableCopy.jar CopyDataFromSmallToLargeTable small large c hbase/copy

* *Validate the files*
hadoop jar SmallToLargeTableCopy.jar ValidateCopyOutput hbase/copy/c large

* *Use HBase Import to move HFiles into Large table*
hadoop jar SmallToLargeTableCopy.jar ImportToLargerTableMain hbase/copy large

