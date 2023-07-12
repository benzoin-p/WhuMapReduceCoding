import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Task2Reducer extends Reducer<IntWritable, Text, Text, Text> {
    int index = 0;

    @Override
    public void reduce(IntWritable key,Iterable<Text> value,
                       Reducer<IntWritable,Text,Text,Text>.Context context)
            throws IOException,InterruptedException {
        index++;
        context.write(new Text(index+" "+key.toString()),new Text(""));
    }
}
