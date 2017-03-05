package BDPA;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.HashMap;

public class Preprocessing extends Configured implements Tool {
	
	
   public static enum COUNTER {
		  COUNT_LINES
		  };
   public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new Preprocessing(), args);
      
      System.exit(res);
   }

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      Job job = new Job(getConf(), "Preprocessing");
      job.setJarByClass(Preprocessing.class);
      job.setOutputKeyClass(LongWritable.class);
      job.setOutputValueClass(Text.class);

      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);
     

      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);
      
      /* set output as csv file */
      job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ",");
      
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      job.waitForCompletion(true);
      
      return 0;
   }
   
   
   
   public static class Map extends Mapper<LongWritable, Text, LongWritable, Text> {
      /*private IntWritable doc_id = new IntWritable();*/
      private Text word = new Text();

      @Override
      public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException {



        /* Initialize a hashset variable for stopwords, set of strings without duplicates*/
        HashSet<String> stopwords = new HashSet<String>();

        /* Read file of stopwords*/
        BufferedReader Reader = new BufferedReader(
                new FileReader(
                        new File(
                                "/home/cloudera/workspace/Assignment1/stopwords.txt")));

        /* Add each line (word) in the variable stopwords*/
        String pattern;
        while ((pattern = Reader.readLine()) != null) {
            stopwords.add(pattern.toLowerCase());
            }
        
        Reader.close();
        
        /* Initialise a hashmap to store each word of the vocabulary and its global 
         * frequency in pg100.txt from the wordcountpg100.txt */
        /*
        HashMap<String, Integer> map_word_count = new HashMap<String, Integer>();
        BufferedReader Reader_count = new BufferedReader(
                new FileReader(
                        new File(
                               "/home/cloudera/workspace/Assignment2/wordcountpg100.txt"
                        		)));
        
        String line;

        while ((line = Reader_count.readLine()) != null)
        {
            String[] parts = line.split(",", 2);
            if (parts.length >= 2)
            {
                String word  = parts[0];
                int count = Integer.parseInt(parts[1]);
                map_word_count.put(word,count);
            } else {
                System.out.println("ignoring line: " + line);
            }
        }
        Reader_count.close();
        */
        
        if (!value.toString().isEmpty())
        {
        for (String token: value.toString().replaceAll("[^a-zA-Z0-9 ]", " ").split("\\s+")) {

          /* if word not in stop words list then we set word with the value then write it into context */
          

          if (!stopwords.contains(token.toLowerCase())) {
                word.set(token.toLowerCase());
                context.write(key, word);
                }
        
           

           
                
        	 	
        

            }
         }
      }
   }

   public static class Reduce extends Reducer<LongWritable, Text, LongWritable, Text> {
      @Override
      public void reduce(LongWritable key, Iterable<Text> values, Context context)
              throws IOException, InterruptedException {
          /* Create for each key, the list of unique words in the line */

         HashSet<String> setvalue = new HashSet<String>();
         /* Add the value for the document in setvalue */

         for (Text val : values)
         {
          setvalue.add(val.toString());
         }
         
         StringBuilder reducedvalue = new StringBuilder();
         for (String val : setvalue) {

            if (reducedvalue.length() !=0){
              reducedvalue.append(' ');
            }

            reducedvalue.append(val);
         }
         
         context.getCounter(COUNTER.COUNT_LINES).increment(1);
         context.write(key, new Text(reducedvalue.toString()));
         
      }
   }
}