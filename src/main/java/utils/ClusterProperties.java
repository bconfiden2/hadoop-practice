package utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Properties;

public class ClusterProperties extends Configured implements Tool {

    public class MyConfigure extends Configuration {
        Properties myProps;

        public MyConfigure(Configuration original) {
            super(original);
            myProps = getProps();
        }

        public void printList() {
            myProps.list(System.out);
        }

        @Override
        protected synchronized Properties getProps() {
            return super.getProps();
        }
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new ClusterProperties(), args);
    }

    @Override
    public int run(String[] strings) throws Exception {

        MyConfigure conf = new MyConfigure(getConf());
        conf.printList();

        return 0;
    }
}
