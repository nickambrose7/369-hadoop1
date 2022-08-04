package csc369;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Report2b {

    public static final Class OUTPUT_KEY_CLASS = LongWritable.class;
    public static final Class OUTPUT_VALUE_CLASS = IntWritable.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, LongWritable, IntWritable> {

        @Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] sa = value.toString().split("\t"); 
        LongWritable resp_code = new LongWritable();
        resp_code.set(Long.parseLong(sa[0]));
        IntWritable num_occurances = new IntWritable();
        num_occurances.set(Integer.parseInt(sa[1]));
        context.write(resp_code, num_occurances);
        }
    }

    public static class ReducerImpl extends Reducer<LongWritable, IntWritable, LongWritable, IntWritable> {
    
        @Override
        protected void reduce(LongWritable resp_code, Iterable<IntWritable> num_occurances,
                    Context context) throws IOException, InterruptedException {
        // This reduce function will get the counts in order.    
            Iterator<IntWritable> itr = num_occurances.iterator();
            while (itr.hasNext()){
                context.write(resp_code, itr.next());
            }
        }
    }
}


