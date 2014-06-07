package synonymSearchEngine.mapReduce.inputFormat;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


public class EnUsDictionaryTextInputFormat extends FileInputFormat<LongWritable, Text>{

	public EnUsDictionaryTextInputFormat() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit arg0,
			TaskAttemptContext arg1) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return new RecordReader<LongWritable, Text>() {

			@Override
			public void close() throws IOException {
				// TODO Auto-generated method stub
				
			}

			@Override
			public LongWritable getCurrentKey() throws IOException,
					InterruptedException {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public Text getCurrentValue()
					throws IOException, InterruptedException {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public float getProgress() throws IOException, InterruptedException {
				// TODO Auto-generated method stub
				return 0;
			}

			@Override
			public void initialize(InputSplit arg0, TaskAttemptContext arg1)
					throws IOException, InterruptedException {
				// TODO Auto-generated method stub
				
			}

			@Override
			public boolean nextKeyValue() throws IOException,
					InterruptedException {
				// TODO Auto-generated method stub
				return false;
			}
		};
	}

}
