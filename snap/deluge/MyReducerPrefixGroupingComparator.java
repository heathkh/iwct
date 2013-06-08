
/*
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;
*/
/*
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;
*/

//import org.apache.hadoop.io.BytesWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;

/**
 * This is a set of support classes to help bridge the java and C++ worlds 
 * with PERT.
 */


public final class MyReducerPrefixGroupingComparator implements RawComparator<Text> {
  public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
    int prefix_len_bytes = 8;
    return WritableComparator.compareBytes(b1, s1, prefix_len_bytes,  b2, s2, prefix_len_bytes);	    
  }

  public int compare(Text o1, Text o2) {
    return compare(o1.getBytes(), 0, o1.getLength(), o2.getBytes(), 0, o2.getLength());
  }
}

