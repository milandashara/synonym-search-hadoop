package synonymSearchEngine.mapReduce;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class CustomTextInputFormat extends FileInputFormat<LongWritable, Text> {

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit arg0,
			TaskAttemptContext arg1) throws IOException, InterruptedException {

		return new RecordReader<LongWritable, Text>() {

			HashMap<Long, String> keyValue = new HashMap<Long, String>();
			int pos = 0;

			@Override
			public void close() throws IOException {

			}

			@Override
			public LongWritable getCurrentKey() throws IOException,
					InterruptedException {
				// TODO Auto-generated method stub
				return new LongWritable(pos);
			}

			@Override
			public Text getCurrentValue() throws IOException,
					InterruptedException {
				// TODO Auto-generated method stub
				Text t=new Text(keyValue.get(new Long(pos)));
				pos++;
				return t;

			}

			@Override
			public float getProgress() throws IOException, InterruptedException {
				// TODO Auto-generated method stub
				return 0;
			}

			@Override
			public void initialize(InputSplit genericSplit,
					TaskAttemptContext context) throws IOException,
					InterruptedException {
				// This InputSplit is a FileInputSplit
				FileSplit split = (FileSplit) genericSplit;
				// Retrieve file containing Split "S"
				final Path file = split.getPath();
				// Retrieve configuration, and Max allowed
				// bytes for a single record
				Configuration job = context.getConfiguration();

				FileSystem fs = file.getFileSystem(job);
				FSDataInputStream fileIn = fs.open(split.getPath());
				StringBuffer inputLine = new StringBuffer();
				String tmp;
				while ((tmp = fileIn.readLine()) != null) {
					inputLine.append(tmp);
			//		System.out.println(tmp);
				}
				String fileInString=inputLine.toString();
				String s[]=fileInString.split("--------------------------------");
				
				for(int i=0;i<s.length;i++)
				{
					keyValue.put(new Long(i), s[i]);
					//System.out.println(s[i]);
				}
				

			}

			@Override
			public boolean nextKeyValue() throws IOException {
				if (keyValue.get(new Long(pos)) == null)
					return false;
				else
					return true;
			}
		};
	}

}
