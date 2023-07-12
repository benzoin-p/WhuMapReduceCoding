import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MapReduceTask2 {
    public static void main(String[] args) throws Exception{
        //把MapReduce作业抽象成Job对象了并提交
        Configuration conf = new Configuration();
        String[] otherArgs = (new GenericOptionsParser(conf,args)).getRemainingArgs();
        if(otherArgs.length<2){
            System.err.println("Usage:CrossTest <in> [..<in>] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf,"对两个文件中的数据进行合并与去重");
        job.setJarByClass(MapReduceTask2.class);

        //设置Job的Mapper相关属性
        job.setMapperClass(Task2Mapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        //设置输入路径
        for(int i = 0; i <otherArgs.length - 1;i++){
            FileInputFormat.addInputPath(job,new Path(otherArgs[i]));
        }

        //设置Job的Reducer相关属性
        job.setReducerClass(Task2Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //设置输出路径
        FileOutputFormat.setOutputPath(job,new Path(otherArgs[otherArgs.length -1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
