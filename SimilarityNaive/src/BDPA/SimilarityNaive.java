package BDPA;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;





import java.io.*;
import java.util.*;


public class SimilarityNaive extends Configured implements Tool {

   
   public static enum COUNTER {
		  COUNT_COMPARAISONS_NAIVE
		  };
		  
		  

   public static void main(String[] args) throws Exception {
	   
      System.out.println(Arrays.toString(args));
      
      
      int res = ToolRunner.run(new Configuration(), new SimilarityNaive(), args);
      
      System.exit(res);
   }

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      Job job = new Job(getConf(), "SimilarityNaive");
      job.setJarByClass(SimilarityNaive.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);

      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);
     

      job.setInputFormatClass(KeyValueTextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      // set key-value separator for the input file
      job.getConfiguration().set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");
      
      /* set output as csv file */
      job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ",");
      
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      job.waitForCompletion(true);

      // Write counter to file
      FileSystem fs = FileSystem.newInstance(getConf());
      long counter = job.getCounters().findCounter(COUNTER.COUNT_COMPARAISONS_NAIVE).getValue();
      Path outFile = new Path(new Path(args[1]),"NB_Comp_SimilarityNaive.txt");
      BufferedWriter writer = new BufferedWriter(
    		  					new OutputStreamWriter(
    		  							fs.create(outFile, true)));
      writer.write(String.valueOf(counter));
      writer.close();
      
      return 0;
   }
   



   
   public static class Map extends Mapper<Text, Text,Text,Text> {
      
      private Text word = new Text();

     /* Initialise one time a hashmap to store key (line id) and the corresponding world*/   
      private static HashMap<String,String> doc_id_contents = new HashMap<String,String>();
     
   
      public Map() throws NumberFormatException, IOException{
     
        /* Default constructor to store pair of line_id content */
       BufferedReader Reader_count = new BufferedReader(
                 new FileReader(
                          new File(
                                  "/home/cloudera/workspace/Assignment2/SimilarityNaive/input/sample.txt"
                              )));
          
          String line;

          while ((line = Reader_count.readLine()) != null)
          {
              String[] parts = line.split(",", 2);
              if (parts.length >= 2)
              {
                 
                  doc_id_contents.put(parts[0],parts[1]);
              
              } else {
                  System.out.println("ignoring line: " + line);
              }
          }
          Reader_count.close();
          
          System.out.println(Arrays.asList(doc_id_contents));
       
        }
      
      @Override
      public void map(Text key, Text value, Context context)
              throws IOException, InterruptedException {

              if (!value.toString().isEmpty()){

              for (String doc_id : doc_id_contents.keySet()) {


            	 
                    // check for not comparing the document with itself and only with document with higher id than itself (to avoid duplicates)
            	  	
            	  	int id_key = Integer.valueOf(key.toString());
            	  	int id_doc = Integer.valueOf(doc_id);
            	  
            	  		
                    if (id_doc > id_key ){
                    
                  
                    StringBuilder keyBuilder = new StringBuilder();
                    keyBuilder.append(key.toString());
                    keyBuilder.append("@");
                    keyBuilder.append(doc_id);


                    
                    context.write(new Text(keyBuilder.toString()), new Text(value.toString()));
                    
        }
      }}
   }

   }
   
   
   public static class Reduce extends Reducer<Text, Text, Text, Text> {

      private static HashMap<String,String> id_contents = new HashMap<String,String>();
     
   
      public Reduce() throws NumberFormatException, IOException{
     
      /* Default constructor to store pair of line_id content */
       BufferedReader Reader_count = new BufferedReader(
                 new FileReader(
                          new File(
                                  "/home/cloudera/workspace/Assignment2/SimilarityNaive/input/sample.txt"
                              )));
          
          String line;

          while ((line = Reader_count.readLine()) != null)
          {
              String[] parts = line.split(",", 2);
              if (parts.length >= 2)
              {
                 
                  id_contents.put(parts[0],parts[1]);
              
              } else {
                  System.out.println("ignoring line: " + line);
              }
          }
          Reader_count.close();
       
        }
     
 
        public static double JaccardSimilarity(Set<String> set1, Set<String> set2){
        // Method to compute the Jaccard similarity between two hashsets 
            Set<String> intersect = new HashSet<>();
            Set<String> union = new HashSet<>();
            intersect.clear();
            intersect.addAll(set1);
            intersect.retainAll(set2);
            union.clear();
            union.addAll(set1);
            union.addAll(set2);
            return (double)intersect.size()/(double)union.size();
        }
	   
  	  @Override
      public void reduce(Text key, Iterable<Text> values, Context context)
              throws IOException, InterruptedException {
    	  
    	  
  		  
        
  		 List <String> list_id = new ArrayList<String>(Arrays.asList(key.toString().split("@")));
         String first_id = list_id.get(0);
         
         /* Get the second id's content */
  		 String second_id = list_id.get(1);
         String second_content = id_contents.get(second_id);
         
         
         // Create set for the words in the first document
         Set<String> setvalue_first = new HashSet<String>(); //Arrays.asList(first_content.split(" ")));
         Set<String> temp = new HashSet<String>();
         
         Iterator<Text> iter = values.iterator();
         while (iter.hasNext())
             temp.add(iter.next().toString());
         
         for (String val : temp.iterator().next().split(" ")){

          setvalue_first.add(val);
         } 
         
         // Create set for the words in the second document
         Set<String> setvalue_second = new HashSet<String>(Arrays.asList(second_content.split(" ")));

          
         // Compute Jaccard Similarity 
         double similarity = JaccardSimilarity(setvalue_first,setvalue_second);

         
         // Increment counter of number of comparisons
         context.getCounter(COUNTER.COUNT_COMPARAISONS_NAIVE).increment(1);

         if (similarity >=0.8){
         StringBuilder ListKeyToText = new StringBuilder();
         ListKeyToText.append(first_id);
         ListKeyToText.append("@");
         ListKeyToText.append(second_id);


         // write key similarity if enough similar
         context.write(new Text(ListKeyToText.toString()),new Text(String.valueOf(similarity)));
       }
      }
      
   }
}
