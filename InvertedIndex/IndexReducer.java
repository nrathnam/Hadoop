package solution;

import java.io.IOException;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

 
public class IndexReducer extends Reducer<Text, Text, Text, Text> {

  private static final String SEP = ",";

  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

    StringBuilder valueList = new StringBuilder();
    boolean firstValue = true;

  
    for (Text value : values) {

   
      if (!firstValue) {
        valueList.append(SEP);
      } else {
        firstValue = false;
      }
      
          valueList.append(value.toString()); 
    }

    
    context.write(key, new Text(valueList.toString()));
  }
}
