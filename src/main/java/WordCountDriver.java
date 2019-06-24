import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCountDriver extends Configured implements Tool {
    public int run(String[] argv) throws Exception {
        if (argv.length != 2) {
            System.out.println("Usage: <input> <output>");
            System.exit(1);
        }

        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.newInstance(conf);
        Path in = new Path(argv[0]);
        Path out = new Path(argv[1]);
        if (fs.exists(out)) {
            fs.delete(out, true);
        }

        Job job = Job.getInstance(conf);
        job.setJarByClass(WordCountDriver.class);
        job.setReducerClass(WordCountReducer.class);
        job.setMapperClass(WordCountMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);

        TextInputFormat.addInputPath(job, in);
        TextOutputFormat.setOutputPath(job, out);

        return job.waitForCompletion(true) ? 0: 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(
                ToolRunner.run(new WordCountDriver(), args)
        );
    }
}
