package solution;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TextPair implements WritableComparable<TextPair> {

    private Text first, second;
    
    public TextPair(Text first, Text second)
    {
         set(first, second);
    }
    public TextPair()
    {
         set(new Text(), new Text());
    }
    public TextPair(String first, String second)
    {
         set(new Text(first), new Text(second));
    }
    
    public void set(Text first, Text second)
    {
        this.first = first; this.second = second;
                
    }
    @Override
    public void readFields (DataInput in) throws IOException
    {
        first.readFields(in); second.readFields(in);
    }
    @Override
    public void write (DataOutput out) throws IOException
    {
        first.write(out); second.write(out);
    }
    
    @Override
    public String toString()
    {
        return first.toString() + "\t" + second.toString();
    }
    
    
    
    public int compareTo(TextPair tpair)
    {
        int cmp = first.compareTo(tpair.first);
        if (cmp != 0) { return cmp;}
        return second.compareTo(tpair.second);
    }
    @Override
    public int hashCode()
    {
        return first.hashCode()*163 + second.hashCode();
    }
     
    public boolean equals(Object o)
    {
        if (o instanceof TextPair)
        {
            TextPair tp = (TextPair) o;
            return first.equals(tp.first) && second.equals(tp.second);
        }
        return false;
    }
}
