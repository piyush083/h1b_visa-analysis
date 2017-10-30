import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class Q3 
{
	public static class Q3Mapper extends Mapper < LongWritable, Text, Text, LongWritable > 
	{
	    LongWritable one = new LongWritable(1);
	    public void map(LongWritable key, Text values, Context context) throws IOException,InterruptedException 
	    {
	        if (key.get() > 0) 
	        {
	            String[] token = values.toString().split("\t");
	            if (token[4].contains("DATA SCIENTIST") && token[1].equals("CERTIFIED")) 
	            {
	                Text answer = new Text(token[3]);
	                context.write(answer, one);
	            }
	        }
	    }
	}
	public static class Q3Reducer extends Reducer < Text, LongWritable, NullWritable, Text > 
	{
	    private TreeMap < LongWritable,
	    Text > DataScientistJobs = new TreeMap < LongWritable,Text > ();

	    public void reduce(Text key, Iterable < LongWritable > values, Context context) throws IOException,InterruptedException 
	    {

	        long sum = 0;
	        for (LongWritable val: values)
	            sum += val.get();

	        DataScientistJobs.put(new LongWritable(sum), new Text(key.toString().replaceAll("\"", "") + "," + sum));
	        if (DataScientistJobs.size() > 5)
	        {
	            DataScientistJobs.remove(DataScientistJobs.firstKey());
	        }
	    }

	    protected void cleanup(Context context) throws IOException,
	    InterruptedException {
	        for (Text t: DataScientistJobs.descendingMap().values())
	        {
	            context.write(NullWritable.get(), t);
	        }

	    }

	}
	public static void main(String args[]) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Data Scientist jobs");

        job.setJarByClass(Q3.class);
        job.setMapperClass(Q3Mapper.class);
        job.setReducerClass(Q3Reducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 1 : 0);
    }
}