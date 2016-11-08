package solution1;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageReducer1 extends Reducer<Text, IntWritable,Text, DoubleWritable>  {
@Override
public void reduce (Text key,Iterable<IntWritable> values , Context context) throws IOException, InterruptedException {
long sum = 0, count = 0;
for ( IntWritable ctr : values ) {
	sum += ctr.get();
	count++;
}
	
if (count != 0) {
    
    double result = (double)sum / (double)count;
   
   context.write(key, new DoubleWritable(result));
 }

	
}
}
