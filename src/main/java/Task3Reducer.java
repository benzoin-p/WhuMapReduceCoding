import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Task3Reducer extends Reducer<Text, Text, Text, Text> {

    static int time = 0;

    @Override
    public void reduce(Text key,Iterable<Text> values,
                       Reducer<Text,Text,Text,Text>.Context context)
            throws IOException,InterruptedException {
        if(time == 0){
            context.write(new Text("grandchild"),new Text("grandparent"));
            time++;
        }
        List<String> grandChildList = new ArrayList<>();
        List<String> grandParentList = new ArrayList<>();
        Iterator iterator = values.iterator();
        while(iterator.hasNext()){
            String value = iterator.next().toString().trim();
            String childName = new String();
            String parentName = new String();
            String type = new String();
            String[] valueList = value.split(" ");
            type = valueList[0];
            childName = valueList[1];
            parentName = valueList[2];
            if(type.equals("0")){
                //type为0说明此时value中的parent为grandparent
                grandParentList.add(parentName);
            }else{
                //type为1说明此时value中的child为grandchild
                grandChildList.add(childName);
            }
        }

        //两者都非空才能使上面的type判断条件成立
        if(grandChildList.size()>0 && grandParentList.size()>0){
            for(String grandChild:grandChildList){
                for(String grandParent:grandParentList){
                    context.write(new Text(grandChild),new Text(grandParent));
                }
            }
        }
    }
}
