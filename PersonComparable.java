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

    public class PersonComparable implements WritableComparable<PersonComparable> {
        public String name;
        public String surname;

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(this.name);
            out.writeUTF(this.surname);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.name = in.readUTF();
            this.surname = in.readUTF();
        }

        @Override
        public int compareTo(PersonComparable that) {
            String thisName = this.name;
            String thatName = that.name;
            return thisName.compareTo(thatName);
        }
    }


