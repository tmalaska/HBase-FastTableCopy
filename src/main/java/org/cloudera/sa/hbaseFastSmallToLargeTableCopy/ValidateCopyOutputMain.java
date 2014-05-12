package org.cloudera.sa.hbaseFastSmallToLargeTableCopy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.util.Bytes;

public class ValidateCopyOutputMain {
  public static void main(String[] args) throws TableNotFoundException, IOException {
    
    if (args.length == 0) {
      System.out.println("ValidateCopyOutput {copyPath} {largeTable}");
      return;
    }
    
    String copyPath = args[0];
    String largeTable = args[1];
    
    Configuration conf = HBaseConfiguration.addHbaseResources(new Configuration());
    
    HBaseAdmin admin = new HBaseAdmin(conf);
    HTableDescriptor largeTD = admin.getTableDescriptor(Bytes
        .toBytes(largeTable));
  
    List<HRegionInfo> regions = admin.getTableRegions(Bytes.toBytes(largeTable));
    ArrayList<byte[]> endKeyList = new ArrayList<byte[]>();
    for (HRegionInfo ri: regions) {
      endKeyList.add(ri.getEndKey());
    }
    endKeyList.add(Bytes.toBytes("99999"));
    Collections.sort(endKeyList,  Bytes.BYTES_COMPARATOR);
    
    FileSystem fs = FileSystem.get(conf);
    
    FileStatus[] storeFiles = fs.listStatus(new Path(copyPath));
    
    
    for (FileStatus storeFile: storeFiles) {
      HFile.Reader hfr = HFile.createReader(fs, storeFile.getPath(),
          new CacheConfig(conf));
      
      System.out.println("File: " + storeFile.getPath());
      System.out.println(" - FirstKey: " + hfr.getFirstKey());
      System.out.println(" - FirstRowKey: " + hfr.getFirstRowKey());
      System.out.println(" - LastKey: " + hfr.getLastKey());
      System.out.println(" - LastRowKey: " + hfr.getLastRowKey());
      System.out.println(" - Entries: " + hfr.getEntries());
      
      int startingRegion = 0;
      for (int i = 0; i < endKeyList.size(); i++) {
        if (Bytes.compareTo(hfr.getFirstRowKey(), endKeyList.get(i)) > 0) {
          startingRegion++;
        } else {
          break;
        }
      }
      
      int endingRegion = 0;
      for (int i = 0; i < endKeyList.size(); i++) {
        if (Bytes.compareTo(hfr.getLastRowKey(), endKeyList.get(i)) > 0) {
          endingRegion++;
        } else {
          break;
        }
      }
      
      System.out.println(" - Regions: " + startingRegion + " - " + endingRegion + " (" + (startingRegion == endingRegion) + ")");
      
      hfr.getLastKey();
      hfr.close();
    }
    
    
    fs.close();
    admin.close();
  }
}
