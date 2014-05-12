package org.cloudera.sa.hbaseFastSmallToLargeTableCopy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.cloudera.sa.hbaseFastSmallToLargeTableCopy.utils.HFileUtils;

public class ImportToLargerTableMain {
  public static void main(String[] args) throws Exception {
    
    if (args.length == 0) {
      System.out.println("ImportToLargerTableMain {originalHFilePath} {largeTableName}");
      return;
    }
    
    String output = args[0];
    String hTableName = args[1];
    
    Configuration config = HBaseConfiguration.create();
    HTable hTable = new HTable(config, hTableName);
    
    FileSystem hdfs = FileSystem.get(config);
    
    //Must all HBase to have write access to HFiles
    HFileUtils.changePermissionR(output, hdfs);
    
    LoadIncrementalHFiles load = new LoadIncrementalHFiles(config);
    load.doBulkLoad(new Path(output), hTable);
  }
}
