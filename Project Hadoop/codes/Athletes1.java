import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Athletes
{
    public static class AMapper extends Mapper<LongWritable,Text,Text,IntWritable>
    {
      private final static IntWritable zero = new IntWritable(0);
      private final static IntWritable one = new IntWritable(1);
      private Text word = new Text();
      public void map(LongWritable key, Text inputCSVLine, Context context) throws IOException, InterruptedException
      {
	int counter = 0;
	String all = null;
	if(((LongWritable)key).get()==0)
	{
	    //Skip the header line (it starts at offset 0)
	    return;
	}
        String[] fields = inputCSVLine.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
        for(int i=0;i<fields.length;i++)
        {
            if(i!=1)
            {
        	if(fields[i].contains("\""))
        	{
        	    fields[i]=fields[i].replace("\"", "");
        	}
            }
        }
        for(String tmp : fields)
        {
            if(counter == 14)
            {
        	word.set(tmp);
        	if(tmp.compareTo("Gold") == 0)
        	{
        	    all = fields[0] + "," + fields[1] + "," + fields[2] + "," + fields[3] + ",";
        	    word.set(all);
        	    context.write(word,one);
        	}
        	else
        	{
        	    all = fields[0] + "," + fields[1] + "," + fields[2] + "," + fields[3]+ ",";
        	    word.set(all);
        	    context.write(word,zero);
        	}
            }
            counter++;
        }
      }
    }
   
    public static class AReducer extends Reducer<Text,IntWritable,Text,IntWritable>
    {
      private IntWritable result = new IntWritable();
      public void reduce(Text word, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException
      {
	  int sum = 0;
	  for (IntWritable val : values)
	  {
	      sum += val.get();
	  }
	  result.set(sum);
	  context.write(word,result);
      }
    }
    
    //-------------------------------------------------------------------2----------------------------------------------------------------------
    public static class GMapper extends Mapper<LongWritable,Text,IntWritable,Text>
    {
      private IntWritable athleteId = new IntWritable();
      public void map(LongWritable key, Text inputLine, Context context) throws IOException, InterruptedException
      {
        String[] fields = inputLine.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
        int athlete_Id = Integer.parseInt(fields[0]);
        String athlete_All=fields[1] + "," + fields[2] + "," + fields[3] + "," + fields[4];
        athleteId.set(athlete_Id);
        context.write(athleteId,new Text(athlete_All));
      }
    }
    
    public static class GReducer extends Reducer<IntWritable,Text,IntWritable,Text>
    {
      public void reduce(IntWritable athleteId, Iterable<Text> athleteAll,Context context) throws IOException, InterruptedException
      {
	Text result = new Text();
        for (Text val : athleteAll)
	{
            String line = val.toString();
            String[] fields = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
            result.set(fields[0] + "," + fields[1] + "," + fields[2] + "," + fields[3]);    
	}
        context.write(athleteId,result);
      }
    }
    
    public static void main(String[] args) throws Exception
    {
      if(args.length!=4)
      {
	  System.err.println("Please provide input and output paths.");
	  System.exit(-1);
      }
      
      Configuration conf1 = new Configuration();
      Job job1 = Job.getInstance(conf1, "countingGoldMedals");
      job1.setJarByClass(Athletes.class);
      job1.setMapperClass(AMapper.class);
      job1.setCombinerClass(AReducer.class);
      job1.setReducerClass(AReducer.class);
      job1.setOutputKeyClass(Text.class);
      job1.setOutputValueClass(IntWritable.class);
      FileInputFormat.addInputPath(job1, new Path(args[1]));
      FileOutputFormat.setOutputPath(job1, new Path(args[2]));
      
      job1.waitForCompletion(true);
      
      Configuration conf2 = new Configuration();
      Job job2 = Job.getInstance(conf2, "sortingAthletes");
      job2.setJarByClass(Athletes.class);
      job2.setMapperClass(GMapper.class);
      job2.setCombinerClass(GReducer.class);
      job2.setReducerClass(GReducer.class);
      job2.setOutputKeyClass(IntWritable.class);
      job2.setOutputValueClass(Text.class);
      FileInputFormat.addInputPath(job2, new Path(args[2]+"/part-r-00000"));
      FileOutputFormat.setOutputPath(job2, new Path(args[3]));
      
      System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}








