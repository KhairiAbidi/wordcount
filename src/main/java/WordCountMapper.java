import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Collections;
import java.util.StringTokenizer;
import java.util.stream.Stream;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private final Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = Collections.list(new StringTokenizer(line))
                .stream()
                .map(token -> (String) token)
                .map(String::toLowerCase)
                .map(word -> word.replaceAll("[^a-zA-Z]",""))
                .filter(word -> word.length() >= 3)
                .toArray(String[]::new);
        for (String w: words) {
            word.set(w);
            context.write(word, one);
        }
    }
}
