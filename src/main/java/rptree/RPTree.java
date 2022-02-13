package rptree;

import Distances;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import writableRPT.*;

import java.io.IOException;
import java.util.*;

import static java.lang.Math.ceil;

public class RPTree extends Configured implements Tool {

    long mor, mob;

    public static void main(String[] args) throws Exception {

        ToolRunner.run(new RPTree(), args);
    }

    @Override
    public int run(String[] strings) throws Exception {
        long t1, t2;

        Configuration conf = getConf();
        String inputPath = conf.get("input");
        String outputPath = conf.get("output");
        FileSystem fs = FileSystem.get(conf);
        int EPOCH = conf.getInt("epoch", 0);

        for(int ep = 0 ; ep < EPOCH ; ep++)
        {
            t1 = System.currentTimeMillis();
            fs.delete(new Path(outputPath + ".0"), true);
            initGraph(inputPath, outputPath + ".0", ep, 0);
            System.out.printf("ep:%2d, it:%2d\t%d\t%d\t%d\n", ep, 0, System.currentTimeMillis() - t1, mor, mob);

            ArrayList<String> leaves = new ArrayList<>();
            int num_leaves = 0;
            int itr = 0;
            long processed = 1;
            while(processed > 0)
            {
                fs.delete(new Path(outputPath + "." + Integer.toString(itr+1)), true);

                conf.setInt("itr", itr);
                t1 = System.currentTimeMillis();
                processed = dnc(outputPath, ep, itr+1);

                fs.delete(new Path(outputPath + "." + Integer.toString(itr)), true);
                itr++;

                RemoteIterator<LocatedFileStatus> it = fs.listLocatedStatus(new Path(outputPath+"."+Integer.toString(itr)));
                while(it.hasNext())
                {
                    String[] wds = it.next().getPath().toString().split("/");
                    if(wds[wds.length-1].substring(0,3).equals("lea")) {
                        leaves.add(wds[wds.length - 1]);
                        num_leaves++;
                        fs.rename(new Path(outputPath+"."+itr+"/"+wds[wds.length-1]), new Path(outputPath+"."+wds[wds.length-1]));
                    }
                }
                System.out.printf("ep:%2d, it:%2d\t%d\t%d\t%d\n", ep, itr, System.currentTimeMillis() - t1, mor, mob);
            }

            fs.delete(new Path(outputPath + "." + Integer.toString(itr)), true);

            fs.delete(new Path(outputPath + ".leaves"), true);
            FSDataOutputStream out = fs.create(new Path(outputPath + ".leaves"));
            for(String x : leaves)
            {
                out.writeBytes(x + "\n");
            }
            out.close();

            fs.delete(new Path(outputPath + ".knn." + ep), true);
            int sz = (int)ceil((float)num_leaves / conf.getInt("mapreduce.job.reduces", 1));
            t1 = System.currentTimeMillis();
            updateKNN(outputPath, ep, sz);
            System.out.printf("ep:%2d, it: K\t%d\t%d\t%d\n", ep, System.currentTimeMillis() - t1, mor, mob);
            if(ep > 0)
            {
                fs.delete(new Path(outputPath + ".knn." + (ep-1)), true);
            }

            for(String x : leaves)
            {
                fs.delete(new Path(outputPath + "." + x), true);
            }
            fs.delete(new Path(outputPath + ".leaves"), true);
        }

        fs.delete(new Path(outputPath + "_result"), true);
        convertResult(outputPath + ".knn." + (EPOCH-1), outputPath + "_result");
        fs.delete(new Path(outputPath + ".knn." + (EPOCH-1)), true);

        return 0;
    }

    /********************************************************************************************/
    /********************************************************************************************/
    /********************************************************************************************/

    public void initGraph(String input, String output, int ep, int itr) throws Exception
    {
        Job job = Job.getInstance(getConf(), "EP: " + ep + ", IT: " + itr);
        job.setJarByClass(RPTree.class);

        job.setMapperClass(initGraphMapper.class);
        job.setCombinerClass(reservoir.class);
        job.setReducerClass(initGraphReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(VectorWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(InfoWritable.class);

        job.setInputFormatClass(TextInputFormat.class);

        LazyOutputFormat.setOutputFormatClass(job, SequenceFileOutputFormat.class);
        MultipleOutputs.addNamedOutput(job, "vectorFile", SequenceFileOutputFormat.class, IntWritable.class, VectorWritable.class);
        MultipleOutputs.addNamedOutput(job, "infoFile", SequenceFileOutputFormat.class, Text.class, InfoWritable.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(false);

        mor = job.getCounters().findCounter(TaskCounter.MAP_OUTPUT_RECORDS).getValue();
        mob = job.getCounters().findCounter(TaskCounter.MAP_OUTPUT_BYTES).getValue();
    }

    /********************************************************************************************/

    public static class initGraphMapper extends Mapper<Object, Text, Text, VectorWritable>
    {
        public int dimVector;
        public int div;
        public float[] vector;
        VectorWritable viw = new VectorWritable();
        IntWritable uw = new IntWritable();
        Text index = new Text("0");

        MultipleOutputs<Text, VectorWritable> mos;

        @Override
        protected void setup(Mapper<Object, Text, Text, VectorWritable>.Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            div = conf.getInt("div", 2);
            VectorWritable.dimVector = dimVector = conf.getInt("dimVector", 0);
            vector = new float[dimVector];

            mos = new MultipleOutputs<Text, VectorWritable>(context);
        }

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, VectorWritable>.Context context) throws IOException, InterruptedException {
            StringTokenizer st = new StringTokenizer(value.toString());

            int v = Integer.parseInt(st.nextToken());
            for(int i = 0 ; i < dimVector ; i++)
            {
                vector[i] = Float.parseFloat(st.nextToken());
            }

            uw.set(v);
            viw.set(vector, "0");
            mos.write("vectorFile", uw, viw, "vec/part");

            viw.path = "1";
            context.write(index, viw);
        }

        @Override
        protected void cleanup(Mapper<Object, Text, Text, VectorWritable>.Context context) throws IOException, InterruptedException {
            mos.close();
        }
    }

    /********************************************************************************************/

    public static class reservoir extends Reducer<Text, VectorWritable, Text, VectorWritable> {

        int div;
        VectorWritable[] vectors;

        Random rnd;

        @Override
        protected void setup(Reducer<Text, VectorWritable, Text, VectorWritable>.Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            div = conf.getInt("div", 0);
            VectorWritable.dimVector = conf.getInt("dimVector", 0);
            rnd = new Random();
            vectors = new VectorWritable[div];
            for(int i = 0 ; i < div ; i++)
            {
                vectors[i] = new VectorWritable(0);
            }
        }

        @Override
        protected void reduce(Text key, Iterable<VectorWritable> values, Reducer<Text, VectorWritable, Text, VectorWritable>.Context context) throws IOException, InterruptedException {
            int cnt = 0;
            for(VectorWritable vw : values)
            {
                if(cnt < div)
                {
                    vectors[cnt].update(vw.vector);
                }
                else
                {
                    int tmp = rnd.nextInt(cnt+1);
                    if(tmp < div)
                    {
                        vectors[tmp].update(vw.vector);
                    }
                }
                cnt++;
            }
            for(int i = 0 ; i < Math.min(div, cnt) ; i++)
            {
                if(i == 0) vectors[0].path = Integer.toString(cnt);
                context.write(key, vectors[i]);
            }
        }
    }

    /********************************************************************************************/

    public static class initGraphReducer extends Reducer<Text, VectorWritable, Text, InfoWritable>
    {
        int div;

        InfoWritable ifw = new InfoWritable();

        Random rnd;
        MultipleOutputs<Text, InfoWritable> mos;

        @Override
        protected void setup(Reducer<Text, VectorWritable, Text, InfoWritable>.Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            InfoWritable.div = div = conf.getInt("div", 2);
            VectorWritable.dimVector = InfoWritable.dimVector = conf.getInt("dimVector", 0);
            ifw.initialize();
            rnd = new Random();
            mos = new MultipleOutputs<Text, InfoWritable>(context);
        }

        @Override
        protected void reduce(Text key, Iterable<VectorWritable> values, Reducer<Text, VectorWritable, Text, InfoWritable>.Context context) throws IOException, InterruptedException {

            int size = 0;
            int cnt = 0;
            for(VectorWritable vw : values)
            {
                if(cnt < div) {
                    ifw.set(cnt, vw.vector);
                }
                else {
                    int tmp = rnd.nextInt(cnt+1);
                    if(tmp < div)
                    {
                        ifw.set(tmp, vw.vector);
                    }
                }
                cnt++;
                size += Integer.parseInt(vw.path);
            }
            ifw.blockSize = size;

            mos.write("infoFile", key, ifw, "info/part");
        }

        @Override
        protected void cleanup(Reducer<Text, VectorWritable, Text, InfoWritable>.Context context) throws IOException, InterruptedException {
            mos.close();
        }
    }

    /********************************************************************************************/
    /********************************************************************************************/
    /********************************************************************************************/

    public long dnc(String path, int ep, int itr) throws Exception {
        Job job = Job.getInstance(getConf(), "EP: " + ep + ", IT: " + itr);
        job.setJarByClass(RPTree.class);

        job.setMapperClass(dncMapper.class);
        job.setCombinerClass(reservoir.class);
        job.setReducerClass(dncReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(VectorWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(InfoWritable.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        LazyOutputFormat.setOutputFormatClass(job, SequenceFileOutputFormat.class);
        MultipleOutputs.addNamedOutput(job, "vectorFile", SequenceFileOutputFormat.class, IntWritable.class, VectorWritable.class);
        MultipleOutputs.addNamedOutput(job, "leafFile", SequenceFileOutputFormat.class, IntWritable.class, VectorWritable.class);
        MultipleOutputs.addNamedOutput(job, "infoFile", SequenceFileOutputFormat.class, Text.class, InfoWritable.class);
        FileInputFormat.addInputPath(job, new Path(path + "." + Integer.toString(itr) + "/vec"));
        FileOutputFormat.setOutputPath(job, new Path(path + "." + Integer.toString(itr+1)));

        job.waitForCompletion(false);

        mor = job.getCounters().findCounter(TaskCounter.MAP_OUTPUT_RECORDS).getValue();
        mob = job.getCounters().findCounter(TaskCounter.MAP_OUTPUT_BYTES).getValue();

        return job.getCounters().findCounter(RPTCounters.NUM_PROCESSED).getValue();
    }

    public static class dncMapper extends Mapper<IntWritable, VectorWritable, Text, VectorWritable>
    {
        int itr, div, dimVector, maxBlockSize;
        Text index = new Text();

        FileSystem fs;
        MultipleOutputs<Text, VectorWritable> mos;

        Text infoKey = new Text();
        InfoWritable infoValue;
        Map<String, InfoWritable> info = new HashMap<String, InfoWritable>();

        @Override
        protected void setup(Mapper<IntWritable, VectorWritable, Text, VectorWritable>.Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            InfoWritable.dimVector = VectorWritable.dimVector = dimVector = conf.getInt("dimVector", 0);
            InfoWritable.div = div = conf.getInt("div", 2);
            maxBlockSize = conf.getInt("leaf", 0);
            fs = FileSystem.get(conf);
            itr = conf.getInt("itr", 0);

            RemoteIterator<LocatedFileStatus> it = fs.listFiles(new Path(conf.get("output")+"."+itr+"/info"), false);
            while(it.hasNext())
            {
                String filename = it.next().getPath().toString();
                SequenceFile.Reader d = new SequenceFile.Reader(conf, SequenceFile.Reader.file(new Path(filename)));
                infoValue = new InfoWritable();
                while(d.next(infoKey, infoValue))
                {
                    info.put(infoKey.toString(), infoValue);
                    infoValue = new InfoWritable();
                }
            }

            mos = new MultipleOutputs<Text, VectorWritable>(context);
        }

        @Override
        protected void map(IntWritable key, VectorWritable value, Mapper<IntWritable, VectorWritable, Text, VectorWritable>.Context context) throws IOException, InterruptedException {

            infoValue = info.get(value.path);
            if(infoValue.blockSize <= maxBlockSize)
            {
                mos.write("leafFile", key, value, "leaf-"+ value.path+"/part");
                return;
            }

            float minVal = Float.MAX_VALUE;
            int minIdx = 0;
            for(int i = 0 ; i < div ; i++)
            {
                float dist = Distances.l2(value.vector, infoValue.centroids[i]);
                if(dist < minVal)
                {
                    minVal = dist;
                    minIdx = i;
                }
            }
            value.path += Integer.toString(minIdx);
            context.getCounter(RPTCounters.NUM_PROCESSED).increment(1);
            mos.write("vectorFile", key, value, "vec/part");

            index.set(value.path);
            value.path = "1";
            context.write(index, value);
        }

        @Override
        protected void cleanup(Mapper<IntWritable, VectorWritable, Text, VectorWritable>.Context context) throws IOException, InterruptedException {
            mos.close();
        }
    }

    /********************************************************************************************/

    public static class dncReducer extends Reducer<Text, VectorWritable, Text, InfoWritable>
    {
        int div;

        InfoWritable ifw = new InfoWritable();

        Random rnd;
        MultipleOutputs<Text, InfoWritable> mos;

        @Override
        protected void setup(Reducer<Text, VectorWritable, Text, InfoWritable>.Context context) throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            InfoWritable.div = div = conf.getInt("div", 2);
            VectorWritable.dimVector = InfoWritable.dimVector = conf.getInt("dimVector", 0);
            ifw.initialize();
            rnd = new Random();
            mos = new MultipleOutputs<Text, InfoWritable>(context);
        }

        @Override
        protected void reduce(Text key, Iterable<VectorWritable> values, Reducer<Text, VectorWritable, Text, InfoWritable>.Context context) throws IOException, InterruptedException {

            int size = 0;
            int cnt = 0;
            for(VectorWritable vw : values)
            {
                if(cnt < div) {
                    ifw.set(cnt, vw.vector);
                }
                else {
                    int tmp = rnd.nextInt(cnt+1);
                    if(tmp < div)
                    {
                        ifw.set(tmp, vw.vector);
                    }
                }
                cnt++;
                size += Integer.parseInt(vw.path);
            }
            ifw.blockSize = size;

            mos.write("infoFile", key, ifw, "info/part");
        }

        @Override
        protected void cleanup(Reducer<Text, VectorWritable, Text, InfoWritable>.Context context) throws IOException, InterruptedException {
            mos.close();
        }
    }

    /********************************************************************************************/
    /********************************************************************************************/
    /********************************************************************************************/

    public void updateKNN(String output, int ep, int N) throws Exception
    {
        Job job = Job.getInstance(getConf(), "EP: " + ep + ", IT: K");
        job.setJarByClass(RPTree.class);

        job.setReducerClass(knnReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(NodeDistanceWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(KNNrptWritable.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        NLineInputFormat.setNumLinesPerSplit(job, N);
        MultipleInputs.addInputPath(job, new Path(output + ".leaves"), NLineInputFormat.class, leafMapper.class);
        if(ep > 0)
        {
            MultipleInputs.addInputPath(job, new Path(output + ".knn."+(ep-1)), SequenceFileInputFormat.class, knnMapper.class);
        }
        FileOutputFormat.setOutputPath(job, new Path(output + ".knn."+(ep)));

        job.waitForCompletion(false);

        mor = job.getCounters().findCounter(TaskCounter.MAP_OUTPUT_RECORDS).getValue();
        mob = job.getCounters().findCounter(TaskCounter.MAP_OUTPUT_BYTES).getValue();
    }

    public static class knnMapper extends Mapper<IntWritable, KNNrptWritable, IntWritable, NodeDistanceWritable> {

        int k;
        NodeDistanceWritable ndw = new NodeDistanceWritable();

        @Override
        protected void setup(Mapper<IntWritable, KNNrptWritable, IntWritable, NodeDistanceWritable>.Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            k = KNNrptWritable.numNeighbors = conf.getInt("k", 0);
        }

        @Override
        protected void map(IntWritable key, KNNrptWritable value, Mapper<IntWritable, KNNrptWritable, IntWritable, NodeDistanceWritable>.Context context) throws IOException, InterruptedException {

            for(int i = 0 ; i < k ; i++)
            {
                ndw.set(value.neighbors[i], value.distances[i]);
                context.write(key, ndw);
            }

        }
    }

    public static class leafMapper extends Mapper<Object, Text, IntWritable, NodeDistanceWritable> {
        int k;
        String output_path;
        IntWritable infoKey = new IntWritable();
        VectorWritable infoValue = new VectorWritable();
        LeafBlock block = new LeafBlock();

        Configuration conf;
        FileSystem fs;

        @Override
        protected void setup(Mapper<Object, Text, IntWritable, NodeDistanceWritable>.Context context) throws IOException, InterruptedException {
            conf = context.getConfiguration();
            VectorWritable.dimVector = LeafBlock.dimVector = conf.getInt("dimVector", 0);
            LeafBlock.maxBlockSize = conf.getInt("leaf", 0);
            block.initialize();

            k = conf.getInt("k", 0);
            output_path = conf.get("output");
            fs = FileSystem.get(conf);
        }

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, IntWritable, NodeDistanceWritable>.Context context) throws IOException, InterruptedException {

            RemoteIterator<LocatedFileStatus> it = fs.listFiles(new Path(output_path+"."+value.toString()), false);
            while(it.hasNext())
            {
                String filename = it.next().getPath().toString();
                SequenceFile.Reader d = new SequenceFile.Reader(conf, SequenceFile.Reader.file(new Path(filename)));
                while(d.next(infoKey, infoValue))
                {
                    block.add(infoKey.get(), infoValue.vector);
                }
            }

            for(int i = 0 ; i < block.blockSize ; i++)
            {
                PriorityQueue<NodeDistanceWritable> pq = new PriorityQueue<>();
                for(int j = 0 ; j < block.blockSize ; j++)
                {
                    if(i == j) continue;
                    float dist = Distances.l2(block.vectors[i], block.vectors[j]);
                    if(pq.size() == k)
                    {
                        if(dist < pq.peek().distance)
                        {
                            pq.poll();
                            pq.add(new NodeDistanceWritable(block.ids[j], dist));
                        }
                    }
                    else
                    {
                        pq.add(new NodeDistanceWritable(block.ids[j], dist));
                    }
                }
                infoKey.set(block.ids[i]);
                for(NodeDistanceWritable ndw : pq)
                {
                    context.write(infoKey, ndw);
                }
            }

            block.reset();
        }
    }

    public static class knnReducer extends Reducer<IntWritable, NodeDistanceWritable, IntWritable, KNNrptWritable> {

        int k;
        KNNrptWritable knw = new KNNrptWritable();

        @Override
        protected void setup(Reducer<IntWritable, NodeDistanceWritable, IntWritable, KNNrptWritable>.Context context) throws IOException, InterruptedException {
            KNNrptWritable.numNeighbors = k = context.getConfiguration().getInt("k", 0);

            knw.setup();
        }

        @Override
        protected void reduce(IntWritable key, Iterable<NodeDistanceWritable> values, Reducer<IntWritable, NodeDistanceWritable, IntWritable, KNNrptWritable>.Context context) throws IOException, InterruptedException {

            PriorityQueue<NodeDistanceWritable> pq = new PriorityQueue<>();
            for(NodeDistanceWritable val : values) {
                pq.add(new NodeDistanceWritable(val.id, val.distance));
            }

            int cnt = 0;
            int bef = -1;
            NodeDistanceWritable ndw;
            while(pq.size() > 0)
            {
                do {
                    ndw = pq.poll();
                } while(ndw != null && (ndw.id == bef || ndw.id == -1));
                if(ndw == null) break;
                bef = ndw.id;

                knw.set(cnt, ndw.id, ndw.distance);
                cnt++;
                if(cnt == k) break;
            }
            while(cnt < k)
            {
                knw.set(cnt, -1, Float.MAX_VALUE);
                cnt++;
            }
            context.write(key, knw);
        }
    }

    /********************************************************************************************/
    /********************************************************************************************/
    /********************************************************************************************/

    public void convertResult(String inputPath, String outputPath) throws Exception {
        Job job = Job.getInstance(getConf(), "Final Result");
        job.setJarByClass(RPTree.class);

        job.setMapperClass(resultMapper.class);
        job.setReducerClass(resultReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(false);
    }

    public static class resultMapper extends Mapper<IntWritable, KNNrptWritable, IntWritable, IntWritable> {

        int k = 0;
        IntWritable kw = new IntWritable();
        IntWritable vw = new IntWritable();

        @Override
        protected void setup(Mapper<IntWritable, KNNrptWritable, IntWritable, IntWritable>.Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            k = KNNrptWritable.numNeighbors = conf.getInt("k", 0);
        }

        @Override
        protected void map(IntWritable key, KNNrptWritable value, Mapper<IntWritable, KNNrptWritable, IntWritable, IntWritable>.Context context) throws IOException, InterruptedException {

            kw.set(key.get());
            for(int i = 0 ; i < k ; i++)
            {
                vw.set(value.neighbors[i]);
                context.write(kw, vw);
            }
        }
    }

    public static class resultReducer extends Reducer<IntWritable, IntWritable, IntWritable, Text>
    {
        Text knn = new Text("");

        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values,
                              Reducer<IntWritable, IntWritable, IntWritable, Text>.Context context)
                throws IOException, InterruptedException {

            String result = "";
            for(IntWritable v : values)
            {
                result += v.get() + " ";
            }
            knn.set(result);
            context.write(key, knn);
        }
    }
}
