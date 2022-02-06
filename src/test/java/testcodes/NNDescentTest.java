package testcodes;

import nndescent.NNDescent;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class NNDescentTest {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInt("dimVector",50);
        conf.setInt("k", 5);
        conf.setInt("sampleSize", 5);
        conf.setInt("numVectors", 500);
        String inputPath = "data/glove_500_input";
        String outputPath = "data/nnd";

        ToolRunner.run(conf, new NNDescent(), new String[]{inputPath, outputPath});
    }
}
