import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Task1Mapper extends Mapper<LongWritable, Text, Text, Text>{

    @Override
    protected void map(LongWritable key, Text value,
                       Mapper<LongWritable, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
        String text = value.toString().trim();
        context.write(new Text(text),new Text(""));
    }
}
