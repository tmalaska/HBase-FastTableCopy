package org.cloudera.sa.hbaseFastSmallToLargeTableCopy;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.server.datanode.DataNode;

public class CreateTablesMain {
	public static void main(String[] args) throws IOException {
		if (args.length == 0) {
			System.out.println("CreateTables {smallTableName} {largeTableName} {columnFamilyName} {smallTableRegionCount} {largeTableRegionCount}");
			return;
		}
		
		String smallTableName = args[0];
		String largeTableName = args[1];
		String columnFamilyName = args[2];
		String smallRegionCount = args[3];
		String largeRegionCount = args[4];
		
		long regionMaxSize = 107374182400l;
		
		Configuration config = HBaseConfiguration.addHbaseResources(new Configuration());
    
		HBaseAdmin admin = new HBaseAdmin(config);
		
		createTable(smallTableName, columnFamilyName, Short.parseShort(smallRegionCount), regionMaxSize, admin);
		createTable(largeTableName, columnFamilyName, Short.parseShort(largeRegionCount), regionMaxSize, admin); 
		
		
		admin.close();
		System.out.println("Done");
	}

	private static void createTable(String tableName, String columnFamilyName,
			short regionCount, long regionMaxSize, HBaseAdmin admin)
			throws IOException {
	  System.out.println("Creating Table: " + tableName);
	  
		HTableDescriptor tableDescriptor = new HTableDescriptor(); 
		tableDescriptor.setName(Bytes.toBytes(tableName));
		
		HColumnDescriptor columnDescriptor = new HColumnDescriptor(columnFamilyName);
		
		columnDescriptor.setCompressionType(Compression.Algorithm.SNAPPY);
		columnDescriptor.setBlocksize(64 * 1024);
		columnDescriptor.setBloomFilterType(BloomType.ROW);
		
		tableDescriptor.addFamily(columnDescriptor);
		
		tableDescriptor.setMaxFileSize(regionMaxSize);
		tableDescriptor.setValue(tableDescriptor.SPLIT_POLICY, ConstantSizeRegionSplitPolicy.class.getName());
		
		tableDescriptor.setDeferredLogFlush(true);
		
		regionCount = (short)Math.abs(regionCount);
		
		int regionRange = Short.MAX_VALUE/regionCount;
		int counter = 0;
		
		byte[][] splitKeys = new byte[regionCount][];
		for (int i = 0 ; i < splitKeys.length; i++) {
			counter = counter + regionRange;
			String key = StringUtils.leftPad(Integer.toString(counter), 5, '0');
			splitKeys[i] = Bytes.toBytes(key); 
			System.out.println(" - Split: " + i + " '" + key + "'");
		}
		
		admin.createTable(tableDescriptor, splitKeys);
	}
}
