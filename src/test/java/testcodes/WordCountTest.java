package testcodes;

import wordcount.WordCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class WordCountTest {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String inputPath = "data/input";
        String outputPath = "data/output";

        ToolRunner.run(conf, new WordCount(), new String[]{inputPath, outputPath});
    }
}
