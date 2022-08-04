package csc369;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class AccessLog {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = IntWritable.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final IntWritable one = new IntWritable(1);

        @Override
	protected void map(LongWritable key, Text value,
			   Context context) throws IOException, InterruptedException {
        /* this function takes as input each line of our access.log file and emits (hostname, 1) for each line.
        Need to understand that the map function is called separatly on each line in our file. */
	    String[] sa = value.toString().split(" "); // Split our value based on spaces.
	    Text hostname = new Text();
	    hostname.set(sa[0]); // first item in
	    context.write(hostname, one);
        }
    }

    public static class ReducerImpl extends Reducer<Text, IntWritable, Text, IntWritable> {
	private IntWritable result = new IntWritable(); // would need to change the type if I wanted 
    //my result to be a different type.
    
        @Override
	protected void reduce(Text hostname, Iterable<IntWritable> intOne,
			      Context context) throws IOException, InterruptedException {
        /* This funcition takes a given hostname and a list of 1's for each time the hostname occured. */
            int sum = 0;
            Iterator<IntWritable> itr = intOne.iterator();
        
            while (itr.hasNext()){
                sum  += itr.next().get(); // add up all the ones.
            }
            result.set(sum);
            context.write(hostname, result); // emit the hostname and amount of times it occurs.
            // output file will be tab delimited.
       }
    }
}
