package org.cloudera.sa.hbaseFastSmallToLargeTableCopy;

import java.io.IOException;
import java.util.Random;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.cloudera.sa.hbaseFastSmallToLargeTableCopy.utils.HFileUtils;
import org.cloudera.sa.hbaseFastSmallToLargeTableCopy.utils.NMapInputFormat;

public class PopulateSmallTableMain {
	
	public static String TABLE_NAME = "custom.table.name";
	public static String COLUMN_FAMILY = "custom.column.family";
	public static String RUN_ID = "custom.runid";
	public static String NUMBER_OF_RECORDS = "custom.number.of.records";
	
	
	public static void main(String[] args) throws Exception {
		
		if (args.length == 0) {
			System.out.println("PopulateSmallTable {numberOfMappers} {numberOfRecords} {tmpOutputPath} {smallTableName} {columnFamily} {runID}");
			return;
		}
				
		String numberOfMappers = args[0];
		String numberOfRecords = args[1];
		String outputPath = args[2];
		String tableName = args[3];
		String columnFamily = args[4];
		String runID = args[5];

		// Create job
		Job job = Job.getInstance();

		job.setJarByClass(PopulateSmallTableMain.class);
		job.setJobName("PopulateSmallTableMain: " + runID);
		job.getConfiguration().set(NUMBER_OF_RECORDS, numberOfRecords);
		
		job.getConfiguration().set(TABLE_NAME, tableName);
		job.getConfiguration().set(COLUMN_FAMILY, columnFamily);
		job.getConfiguration().set(RUN_ID, runID);
		
		// Define input format and path
		job.setInputFormatClass(NMapInputFormat.class);
		NMapInputFormat.setNumMapTasks(job.getConfiguration(), Integer.parseInt(numberOfMappers));

		Configuration config = HBaseConfiguration.create();

		HTable hTable = new HTable(config, tableName);

		// Auto configure partitioner and reducer
		HFileOutputFormat.configureIncrementalLoad(job, hTable);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		// Define the mapper and reducer
		job.setMapperClass(CustomMapper.class);
		// job.setReducerClass(CustomReducer.class);

		// Define the key and value format
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(KeyValue.class);

		// Exit
		job.waitForCompletion(true);
		FileSystem hdfs = FileSystem.get(config);

		// Must all HBase to have write access to HFiles
		HFileUtils.changePermissionR(outputPath, hdfs);

		LoadIncrementalHFiles load = new LoadIncrementalHFiles(config);
		load.doBulkLoad(new Path(outputPath), hTable);

	}

	public static class CustomMapper extends
			Mapper<NullWritable, NullWritable, ImmutableBytesWritable, KeyValue> {
		ImmutableBytesWritable hKey = new ImmutableBytesWritable();
		KeyValue kv;

		Pattern p = Pattern.compile("\\|");
		byte[] columnFamily;
		
		String runID;
		int taskId;
		int numberOfRecords;
		
		@Override
		public void setup(Context context) {
			columnFamily = Bytes.toBytes(context.getConfiguration().get(COLUMN_FAMILY));
			runID = context.getConfiguration().get(RUN_ID);
			taskId = context.getTaskAttemptID().getTaskID().getId();
			numberOfRecords = context.getConfiguration().getInt(NUMBER_OF_RECORDS, 1000) / context.getConfiguration().getInt("nmapinputformat.num.maps", 1);
		}

		Random r = new Random();
		
		@Override
		public void map(NullWritable key, NullWritable value, Context context)
				throws IOException, InterruptedException {
			
			for (int i = 0; i < numberOfRecords; i++) {
				String keyRoot = StringUtils.leftPad(Integer.toString(r.nextInt(Short.MAX_VALUE)), 5, '0');
				
				hKey.set(Bytes.toBytes(keyRoot + "|" + runID + "|" + taskId ));
		        
		        kv = new KeyValue(hKey.get(), columnFamily,
		            Bytes.toBytes("C"), Bytes.toBytes(numberOfRecords));
	
		        context.write(hKey, kv);
			}
		}
	}
}
