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

    public class PersonPartitioner extends Partitioner<PersonComparable, Text>{
        @Override
        public int getPartition(PersonComparable key, Text value, int numPartitions){
            return Math.abs( (key.surname).hashCode() ) % numPartitions;
        }
    }
