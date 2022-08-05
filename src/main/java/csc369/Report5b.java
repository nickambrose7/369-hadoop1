package csc369;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Report5b {

    public static final Class OUTPUT_KEY_CLASS = LongWritable.class;
    public static final Class OUTPUT_VALUE_CLASS = Text.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, LongWritable, Text> {

        @Override
	protected void map(LongWritable key, Text value, // map function args: (line in the file, string at that line)
			   Context context) throws IOException, InterruptedException {
        // this is all just getting our info from the last output file.
	    String[] sa = value.toString().split("\t"); // gets us the hostname context split it by tabs.
	    Text hostname = new Text();
            hostname.set(sa[0]);
            LongWritable count = new LongWritable(Long.parseLong(sa[1]));
	    context.write(count, hostname); // literally output the same thing so that our shuffle does it's magic
        }
    }

    
    public static class ReducerImpl extends Reducer<LongWritable, Text, Text, LongWritable> {
	
        @Override
	protected void reduce(LongWritable count, Iterable<Text> hostnames,
			      Context context) throws IOException, InterruptedException {
        // This reduce function will get the counts in order.    
            Iterator<Text> itr = hostnames.iterator();
            while (itr.hasNext()){
                context.write(itr.next(), count);
            }
        }
    }

}
