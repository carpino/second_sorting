package ptollot;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.WritableComparable;

import java.io.*;
import org.apache.hadoop.mapreduce.Partitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SecondarySort
{
    
  public static class NewMapper extends Mapper<Object, Text, PersonComparable, Text>
  {
    //private Text name = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String line = value.toString();
            String[] words = line.trim().split("\\s+");
            PersonComparable person = new PersonComparable();
            person.name = words[1];
            person.surname = words[0];
            Text name = new Text();
            name.set(person.name);
            context.write(person, name);
		}
	}

	public static class NewReducer extends Reducer<PersonComparable, Text, Text, Text>
	{
        private Text out_surname = new Text();

        public void reduce(PersonComparable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            out_surname.set(key.surname);
            for (Text name : values) {
                context.write(name, out_surname);
            }
        }
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		Job job = new Job(conf, "secondsort");
		job.setJarByClass(SecondarySort.class);

        job.setMapOutputKeyClass(PersonComparable.class);
        job.setPartitionerClass(PersonPartitioner.class);
        job.setSortComparatorClass(PersonComparator.class);
        job.setGroupingComparatorClass(GroupComparator.class);

		job.setOutputKeyClass(PersonComparable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(NewMapper.class);
		job.setReducerClass(NewReducer.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
