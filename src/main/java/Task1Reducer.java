import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Task1Reducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key,Iterable<Text> value,
                       Reducer<Text,Text,Text,Text>.Context context)
            throws IOException,InterruptedException {
        context.write(key,new Text(""));
    }
}
