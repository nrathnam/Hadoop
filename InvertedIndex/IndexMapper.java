package solution;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;

public class IndexMapper extends Mapper<Text, Text, Text, Text> {

  @Override
  public void map(Text key, Text value, Context context) throws IOException,
      InterruptedException {

  
    FileSplit fileSplit = (FileSplit) context.getInputSplit();
    Path path = fileSplit.getPath();
    
 
    String wordPlace = path.getName() + "@" + key.toString();
    Text location = new Text(wordPlace);
    
  
    String lc_line = value.toString().toLowerCase();
    
 
    for (String word : lc_line.split("\\W+")) {
      if (word.length() > 0) {
        context.write(new Text(word), location);
      }
    }
  }
}
