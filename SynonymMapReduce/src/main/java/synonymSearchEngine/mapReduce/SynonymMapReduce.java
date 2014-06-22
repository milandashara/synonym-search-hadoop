package synonymSearchEngine.mapReduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;

import synonymSearchEngine.mapReduce.inputFormat.Dictionary1TextInputFormat;

public class SynonymMapReduce {
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("usage: [input] [output]");
			System.exit(-1);
		}

		System.setProperty("conf.HiveConf",
				"/home/cloudera/git/synonym-search-hadoop/SynonymMapReduce/conf/hive-site.xml");

		Configuration conf = new Configuration();
		String keyword="difficult";
		keyword.toUpperCase();
		conf.set("keyword", keyword);
		FileSystem.getLocal(conf).delete(new Path(args[1]), true);

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

		
		  FileInputFormat.setInputPaths(job, new Path(args[0]));
		 
		  /*  FileOutputFormat.setOutputPath(job, new Path(args[1]));
		  MultipleOutputs.addNamedOutput(job, "synonyms",
		  TextOutputFormat.class, Text.class, Text.class);*/
		 
		HCatOutputFormat.setOutput(job,
				OutputJobInfo.create("synonymdb", "synonyms", null));
/*		HCatOutputFormat.setOutput(job,
				OutputJobInfo.create("synonym", "synonym", null));*/
		HCatSchema s = HCatOutputFormat.getTableSchema(job);
		System.err.println("INFO: output schema explicitly set for writing:"
				+ s);
		HCatOutputFormat.setSchema(job, s);
		job.setJarByClass(SynonymMapReduce.class);

		job.submit();

	}
}