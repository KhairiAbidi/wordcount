import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.stream.StreamSupport;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private final IntWritable frequency = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int s = StreamSupport.stream(values.spliterator(), false)
                .map(IntWritable::get)
                .mapToInt(Integer::intValue)
                .sum();
        frequency.set(s);
        context.write(key, frequency);
    }
}
