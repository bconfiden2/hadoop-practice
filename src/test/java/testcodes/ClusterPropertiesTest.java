package testcodes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import utils.ClusterProperties;

public class ClusterPropertiesTest {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        ToolRunner.run(conf, new ClusterProperties(), args);
    }
}
