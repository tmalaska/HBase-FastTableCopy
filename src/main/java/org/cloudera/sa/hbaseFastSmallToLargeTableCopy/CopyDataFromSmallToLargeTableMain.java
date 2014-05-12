package org.cloudera.sa.hbaseFastSmallToLargeTableCopy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFileDataBlockEncoder;
import org.apache.hadoop.hbase.io.hfile.HFileDataBlockEncoderImpl;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import com.google.common.primitives.UnsignedBytes;

public class CopyDataFromSmallToLargeTableMain {

  public static final String CONF_LARGE_TABLE = "custom.large.table";
  public static final String CONF_COLUMN_FAMILY = "custom.column.family";
  public static final String CONF_OUTPUT_PATH = "custom.output.path";

  public static void main(String[] args) throws IOException,
      InterruptedException, ClassNotFoundException {
    if (args.length == 0) {
      System.out
          .println("CopyDataFromSmallToLargeTable {smallTable} {largeTable} {ColumnFamily} {outputPath}");
      return;
    }

    String smallTable = args[0];
    String largeTable = args[1];
    String columnFamily = args[2];
    String outputPath = args[3];
    
    Job job = Job.getInstance();
    
    HBaseConfiguration.addHbaseResources(job.getConfiguration());
    
    job.getConfiguration().set(CONF_LARGE_TABLE, largeTable);
    job.getConfiguration().set(CONF_COLUMN_FAMILY, columnFamily);
    job.getConfiguration().set(CONF_OUTPUT_PATH, outputPath);

    job.setJarByClass(CopyDataFromSmallToLargeTableMain.class);
    job.setJobName("CopyDataFromSmallToLargeTable ");

    Scan scan = new Scan();
    scan.setCaching(500); // 1 is the default in Scan, which will be bad for
                          // MapReduce jobs
    scan.setCacheBlocks(false); // don't set to true for MR jobs

    TableMapReduceUtil.initTableMapperJob(smallTable, // input HBase table name
        scan, // Scan instance to control CF and attribute selection
        MyMapper.class, // mapper
        null, // mapper output key
        null, // mapper output value
        job);
    job.setOutputFormatClass(NullOutputFormat.class); // because we aren't
                                                      // emitting anything from
                                                      // mapper

    job.setNumReduceTasks(0);
    
    boolean b = job.waitForCompletion(true);
  }

  public static class MyMapper extends TableMapper<Text, Text> {

    FileSystem fs;
    CacheConfig cacheConf;
    Algorithm compression;
    BloomType bloomFilterType;
    HFileDataBlockEncoder dataBlockEncoder;
    StoreFile.Writer storeFileWriter;
    int blocksize;
    HColumnDescriptor familyDescriptor;
    int hFileCounter = 0;
    String uniqueName;
    String outputPath;
    int currentWritingToRegion = 0;
    ArrayList<byte[]> endKeyList;
    
    @Override
    public void setup(Context context) throws TableNotFoundException,
        IOException {
      Configuration conf = context.getConfiguration();
      conf = HBaseConfiguration.addHbaseResources(conf);
      
      String largeTable = conf.get(CONF_LARGE_TABLE);
      String columnFamily = conf.get(CONF_COLUMN_FAMILY);
      String outputPath = conf.get(CONF_OUTPUT_PATH);

      HBaseAdmin admin = new HBaseAdmin(conf);
      HTableDescriptor largeTD = admin.getTableDescriptor(Bytes
          .toBytes(largeTable));
    
      List<HRegionInfo> regions = admin.getTableRegions(Bytes.toBytes(largeTable));
      endKeyList = new ArrayList<byte[]>();
      for (HRegionInfo ri: regions) {
        endKeyList.add(ri.getEndKey());
      }
      endKeyList.add(Bytes.toBytes("99999"));
      Collections.sort(endKeyList,  Bytes.BYTES_COMPARATOR);
      
      prepValueForStoreFileWriter(conf, columnFamily, largeTD, outputPath);
      
      fs.mkdirs(new Path(outputPath));
      
      uniqueName = largeTable + "." + context.getTaskAttemptID().getTaskID().getId();
      
      admin.close();
    }

    private void createNewStoreFileWriter(Configuration conf, int regionNum, String outputRootPath)
        throws IOException {
      storeFileWriter = new StoreFile.WriterBuilder(conf,
          cacheConf, fs, blocksize).withFilePath(new Path(outputRootPath + "/" + uniqueName + "." + hFileCounter++ + "." + regionNum))
          .withCompression(compression).withDataBlockEncoder(dataBlockEncoder)
          .withBloomType(bloomFilterType)
          .withChecksumType(Store.getChecksumType(conf))
          .withBytesPerChecksum(Store.getBytesPerChecksum(conf)).build();
    }

    private void prepValueForStoreFileWriter(Configuration conf,
        String columnFamily, HTableDescriptor largeTD,
        String outputPath) throws IOException {
      familyDescriptor = largeTD.getFamily(Bytes
          .toBytes(columnFamily));

      blocksize = familyDescriptor.getBlocksize();

      fs = FileSystem.get(conf);
      cacheConf = new CacheConfig(conf);
      compression = familyDescriptor.getCompression();
      bloomFilterType = familyDescriptor.getBloomFilterType();
      dataBlockEncoder = new HFileDataBlockEncoderImpl(
          familyDescriptor.getDataBlockEncodingOnDisk(),
          familyDescriptor.getDataBlockEncoding());
      this.outputPath = outputPath;
    }

    @Override
    public void map(ImmutableBytesWritable row, Result value, Context context)
        throws InterruptedException, IOException {
      
      if (storeFileWriter == null) {
        
        for (int i = 0 ; i < endKeyList.size(); i++) {
          if (Bytes.compareTo(endKeyList.get(i), row.get()) <= 0) {
            currentWritingToRegion++;
          } else {
            break;
          }
        }
        System.out.println();
        System.out.println("Writing to regions: " + 
           currentWritingToRegion + " " + 
           Bytes.toString(row.get()) );
        System.out.println();
        createNewStoreFileWriter(context.getConfiguration(), currentWritingToRegion, outputPath);
      } else {
        if (Bytes.compareTo(endKeyList.get(currentWritingToRegion), row.get()) <= 0) {
          
          storeFileWriter.close();
          for (int i = currentWritingToRegion ; i < endKeyList.size(); i++) {
            if (Bytes.compareTo(endKeyList.get(i), row.get()) <= 0) {
              currentWritingToRegion++;
            } else {
              break;
            }
          }
          System.out.println();
          System.out.println("Writing to regions: " + 
              currentWritingToRegion + " " + 
              Bytes.toString(row.get()) + " " );
          System.out.println();
          createNewStoreFileWriter(context.getConfiguration(), currentWritingToRegion, outputPath);
        }
      }
      
      for (KeyValue kv: value.raw()) {
        System.out.print(".");
        storeFileWriter.append(kv);
      }
    }
    
    @Override
    public void cleanup(Context context) throws IOException {
      storeFileWriter.close();
    }
  }
}
