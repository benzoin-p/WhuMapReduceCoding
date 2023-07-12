import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Task3Mapper extends Mapper<LongWritable, Text, Text, Text>{

    @Override
    protected void map(LongWritable key, Text value,
                       Mapper<LongWritable, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
        String childName;
        String parentName;
        //type为0表示key为子代，为1表示key为父代
        String type;
        String line = value.toString().trim();
        String[] values = line.split("\\s+");
        if(!values[0].equals("child")){
            childName = values[0];
            parentName = values[1];
            type = "0";
            context.write(new Text(childName),new Text(type+" "+childName+" "+parentName));
            type = "1";
            context.write(new Text(parentName),new Text(type+" "+childName+" "+parentName));
        }
    }
}
