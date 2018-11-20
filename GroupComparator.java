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

    public class GroupComparator extends WritableComparator
    {
        protected GroupComparator() {
            super(PersonComparable.class, true);
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2)
        {
            PersonComparable p1 = (PersonComparable)w1;
            PersonComparable p2 = (PersonComparable)w2;
            String thisName = p1.name;
            String thatName = p2.name;
            return thisName.compareTo(thatName);
        }
    }
