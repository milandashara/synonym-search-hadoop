/**
 * Copyright 2012 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package synonymSearchEngine.mapReduce;

/**
 * <Prove description of functionality provided by this Type> 
 * @author amresh.singh
 */
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordMapper extends Mapper<Object, Text, Text, IntWritable> {
	private Text word = new Text();
	private final static IntWritable one = new IntWritable(1);

	@Override
	public void map(Object key, Text value, Context contex) throws IOException,
			InterruptedException {
		// Break line into words for processing
		String keyword = contex.getConfiguration().get("keyword");
		if (value.toString().contains(keyword)) {
			System.out.println(value.toString());
			word.set(value.toString());
			contex.write(word, one);
		}
		/*
		 * StringTokenizer wordList = new StringTokenizer(value.toString()); //
		 * System.out.println(value.toString()); while
		 * (value.toString().contains(keyword)) {
		 * 
		 * word.set(wordList.nextToken()); contex.write(word, one); }
		 */
	}
}
