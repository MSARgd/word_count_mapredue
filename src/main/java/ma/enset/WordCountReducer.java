package ma.enset;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class WordCountReducer extends Reducer<Text, LongWritable,Text,LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        Iterator<LongWritable> iterator = values.iterator();
        int sum=0;
        while (iterator.hasNext()){
            sum+=iterator.next().get();
        }
        context.write(key,new LongWritable(sum));
    }
}
