package synonymSearchEngine.mapReduce;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;

import synonymSearchEngine.mapReduce.inputFormat.Dictionary1TextInputFormat;

public class SynonymMapReduce {
	
	public static String tableName = "synonyms";
	public static String dbName = "synonymdb";
	
	public static void main(String[] args) throws Exception {

		SynonymMapReduce mapReduce=new SynonymMapReduce();
		mapReduce.startSynonymSearchMapReduce("difficult");
	}

	public void startSynonymSearchMapReduce(String searchKeyword) {

		System.setProperty("conf.HiveConf",
				"/home/cloudera/git/synonym-search-hadoop/SynonymMapReduce/conf/hive-site.xml");
		try {
		Configuration conf = new Configuration();
		String keyword = searchKeyword;
		keyword.toUpperCase();
		conf.set("keyword", keyword);
		FileSystem.getLocal(conf).delete(new Path("output"), true);

		conf.set("textinputformat.record.delimiter", ":");
		Job job = Job.getInstance(conf);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setMapperClass(SynonymMapper.class);
		job.setReducerClass(SynonymReducer.class);

		job.setInputFormatClass(Dictionary1TextInputFormat.class);

		// job.setOutputFormatClass(TextOutputFormat.class);
		// Ignore the key for the reducer output; emitting an HCatalog record as
		// value
		job.setOutputKeyClass(WritableComparable.class);
		job.setOutputValueClass(DefaultHCatRecord.class);
		job.setOutputFormatClass(HCatOutputFormat.class);

		
			FileInputFormat.setInputPaths(job, new Path("input"));

			/*
			 * FileOutputFormat.setOutputPath(job, new Path(args[1]));
			 * MultipleOutputs.addNamedOutput(job, "synonyms",
			 * TextOutputFormat.class, Text.class, Text.class);
			 */
			HashMap<String, String> partition = new HashMap<String, String>();
			partition.put("synonym", searchKeyword);
			HCatOutputFormat.setOutput(job,
					OutputJobInfo.create(dbName, tableName, partition));
			/*
			 * HCatOutputFormat.setOutput(job, OutputJobInfo.create("synonym",
			 * "synonym", null));
			 */
			HCatSchema s = HCatOutputFormat.getTableSchema(job);
			System.err
					.println("INFO: output schema explicitly set for writing:"
							+ s);
			HCatOutputFormat.setSchema(job, s);
			job.setJarByClass(SynonymMapReduce.class);

			job.submit();
		}

		catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}