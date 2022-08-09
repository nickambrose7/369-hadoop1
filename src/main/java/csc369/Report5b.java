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
    public static final Class OUTPUT_VALUE_CLASS = LongWritable.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, LongWritable, LongWritable> {

        @Override
	protected void map(LongWritable key, Text value, // map function args: (line in the file, string at that line)
			   Context context) throws IOException, InterruptedException {
        // this is all just getting our info from the last output file.
	    String[] sa = value.toString().split("\t"); // gets us the hostname context split it by tabs.
	    LongWritable m_or_y = new LongWritable();
        m_or_y.set(Long.parseLong(sa[0]));
            LongWritable count = new LongWritable(Long.parseLong(sa[1]));
	    context.write(m_or_y, count); // literally output the same thing so that our shuffle does it's magic
        }
    }

    
    public static class ReducerImpl extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
	
        @Override
	protected void reduce(LongWritable count, Iterable<LongWritable> m_or_y,
			      Context context) throws IOException, InterruptedException {
        // This reduce function will get the counts in order.    
            Iterator<LongWritable> itr = m_or_y.iterator();
            while (itr.hasNext()){
                context.write(count, itr.next());
            }
        }
    }

}
