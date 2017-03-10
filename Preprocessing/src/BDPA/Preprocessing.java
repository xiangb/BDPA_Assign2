package BDPA;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
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



import java.io.*;
import java.util.*;


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

      // Write counter to file
      FileSystem fs = FileSystem.newInstance(getConf());
      long counter = job.getCounters().findCounter(COUNTER.COUNT_LINES).getValue();
      Path outFile = new Path(new Path(args[1]),"NB_LINES_AFTER_Preprocessing.txt");
      BufferedWriter writer = new BufferedWriter(
    		  					new OutputStreamWriter(
    		  							fs.create(outFile, true)));
      writer.write(String.valueOf(counter));
      writer.close();
      
      return 0;
   }
   



   
   public static class Map extends Mapper<LongWritable, Text, LongWritable, Text> {
      
      private Text word = new Text();
      private HashSet<String> stopwords = new HashSet<String>();
      
      public Map() throws NumberFormatException, IOException{
    	  // Default constructor to load one time the stop words file
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
    	
      }
      @Override
      public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException {

        

        
        	//if the line is not empty, parse it
        for (String token: value.toString().replaceAll("[^a-zA-Z0-9 ]", " ").split("\\s+")) {

          /* if word not in stop words list then we set word with the value then write it into context */
          

          if (!stopwords.contains(token.toLowerCase())) {
        	    // if token only contains a blank character we do not write it 
        	  
              
        	  	
                word.set(token.toLowerCase());
                context.write(key, word);
        	  	}
                
        
           }
         
      }
   }

   

   
   
   public static class Reduce extends Reducer<LongWritable, Text, LongWritable, Text> {
     
	   /* Initialise one time a hashmap to store each word of the vocabulary and its global 
        * frequency in pg100.txt from the wordcountpg100.txt */   
	 private static HashMap<String,Integer> map_word_count = new HashMap<String,Integer>();
	 
	 public Reduce() throws NumberFormatException, IOException{
	 	 
	      /*Default constructor to store (word,frequency) pair 
	       * in the created hashmap from the file wordcountpg100.txt */

		   BufferedReader Reader_count = new BufferedReader(
		             new FileReader(
		                      new File(
		                              "/home/cloudera/workspace/BDPA_Assign2_BXIANG/wordcountpg100.txt"
		                      		)));
		      
		      String line;

		      while ((line = Reader_count.readLine()) != null)
		      {
		          String[] parts = line.split(",", 2);
		          if (parts.length >= 2)
		          {
		             
		              map_word_count.put(parts[0].toString(),new Integer (parts[1]));
		          
		          } else {
		              System.out.println("ignoring line: " + line);
		          }
		      }
		      Reader_count.close();
		   
	   } 


    public static <K, V extends Comparable<? super V>> LinkedHashSet<String> 
    sortByValue( HashMap<K, V> map ){
        List<java.util.Map.Entry<K, V>> list = new LinkedList<>( map.entrySet() );
        
        // sort the list of pairs 

        Collections.sort( list, new Comparator<java.util.Map.Entry<K, V>>()
        {
            
            public int compare( java.util.Map.Entry<K, V> o1, java.util.Map.Entry<K, V> o2 )
            {
                return (o1.getValue()).compareTo(o2.getValue());
            }
        } );
        
        // Create LinkedHashset to store the word in ascending order

        LinkedHashSet<String> result = new LinkedHashSet<String>();

        for (java.util.Map.Entry<K, V> entry : list)
        {
            result.add(entry.getKey().toString());
        }
        
        return result;
    }
	   
  	 @Override
      public void reduce(LongWritable key, Iterable<Text> values, Context context)
              throws IOException, InterruptedException {
    	  
    	  

    	  
 
          /*Create a reduced hashmap where each key is a word for the same 
           * mapper key and the value is the global frequency with the static hashmap 
           * word_word_count containing the global frequency of word in pg100.txt*/

         HashMap<String, Integer> map_word_count_key = new HashMap<String, Integer>();
         
         for (Text val : values)
         {
        	 /*store the global frequency of each word for words corresponding to a same key*/
          map_word_count_key.put(val.toString(),map_word_count.get(val.toString()));

         }

         
         // Sort Hashmap and return a LinkedHashset (to keep the order) with word in ascending order 
         // Using the sortByValue method 
	      
         LinkedHashSet<String> setvalue = new LinkedHashSet<String>();
         
         setvalue = sortByValue(map_word_count_key);
         
         /* Concatenate the words in ascending order of frequency */
         
         StringBuilder reducedvalue = new StringBuilder();
         for (String val : setvalue) {

            if (reducedvalue.length() !=0){
              reducedvalue.append(' ');
            }

            reducedvalue.append(val);
         }
         

         // write for each line the words in the ascending order if not empty
         if(!reducedvalue.toString().isEmpty()){
             // Increment counter
         context.getCounter(COUNTER.COUNT_LINES).increment(1);
         context.write(key, new Text(reducedvalue.toString()));
         }
         
       }
      
   }
}