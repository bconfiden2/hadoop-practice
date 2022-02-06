package nndescent;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
//import sun.awt.X11.XSystemTrayPeer;

import java.io.IOException;
import java.util.*;

public class NNDescent extends Configured implements Tool {

    long mor, mob;

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new NNDescent(), args);
    }

    @Override
    public int run(String[] args) throws Exception {

        long t;

        String inputPath = args[0];
        String outputPath = args[1];
        String randomGraphTmpPath = outputPath + ".rg1";
        String randomGraphPath = outputPath + ".r0";

        FileSystem fs = FileSystem.get(getConf());
        fs.delete(new Path(randomGraphTmpPath), true);
        fs.delete(new Path(randomGraphPath), true);

        t = System.currentTimeMillis();
        runRandomStep1(inputPath, randomGraphTmpPath);
        System.out.println("r0s1: " + (System.currentTimeMillis() - t));

        t = System.currentTimeMillis();
        runRandomStep2(randomGraphTmpPath, randomGraphPath);
        System.out.println("r0s2: " + (System.currentTimeMillis() - t));

        fs.delete(new Path(randomGraphTmpPath), true);

        int round=0;
        long numChanges = 1;
        while(numChanges > 0){

            String roundInputPath = outputPath + ".r" + round;
            String roundTmpPath = outputPath + ".r" + round + ".t";
            String roundOutputPath = outputPath + ".r" + (++round);
            String unsamplePath = outputPath + ".unsampled";

            fs.delete(new Path(roundTmpPath), true);
            fs.delete(new Path(roundOutputPath), true);
            fs.delete(new Path(unsamplePath), true);

            t = System.currentTimeMillis();
            run_step1(roundInputPath, roundTmpPath, round);
            //step, round, time(ms), mor, mob
            System.out.printf("step1\t%d\t%d\t%d\t%d\n", round, (System.currentTimeMillis() - t), mor, mob);

            boolean unsam = false;
            if(fs.exists(new Path(roundTmpPath + "/unsampled")))
            {
                unsam = true;
                fs.rename(new Path(roundTmpPath + "/unsampled"), new Path(unsamplePath));
            }

            t = System.currentTimeMillis();
            Counters cntr = run_step2(roundTmpPath, unsamplePath, unsam, roundOutputPath, round);
            numChanges = cntr.changes;
            long numNodes = cntr.nodes;
            long numFixedNodes = cntr.edges;

            //step, round, time(ms), mor, mob, numChanges
            System.out.printf("step2\t%d\t%d\t%d\t%d\t%d\t%d\t%d\n", round, (System.currentTimeMillis() - t), mor, mob, numChanges, numNodes, numFixedNodes);

            fs.delete(new Path(roundInputPath), true);
            fs.delete(new Path(roundTmpPath), true);
        }
        
        System.out.print("\nConverting Result to TextFile .... ");
        String final_output = outputPath + "_result";
        fs.delete(new Path(final_output), true);
        write_result(outputPath + ".r" + round, final_output);
        fs.delete(new Path(outputPath + ".r" + round), true);
        System.out.println("Complete");
        
        return 0;
    }
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public void runRandomStep1(String inputPath, String outputPath) throws Exception{
        Job job = Job.getInstance(getConf(), "RandomGraphGen-step1");
        job.setJarByClass(NNDescent.class);

        job.setMapperClass(RandomGraphStep1Mapper.class);
        job.setReducerClass(RandomGraphStep1Reducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(NodeOrNodeVectorWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(NodeVectorWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(false);
    }

    public static class RandomGraphStep1Mapper extends Mapper<Object, Text, IntWritable, NodeOrNodeVectorWritable>{

        public int dimVector, k, numVectors;
        public float[] u_vec;
        Random rand = new Random();

        NodeOrNodeVectorWritable nv = new NodeOrNodeVectorWritable();
        IntWritable ok = new IntWritable();

        @Override
        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            dimVector = conf.getInt("dimVector",0);
            k = conf.getInt("k", 0);
            numVectors = conf.getInt("numVectors", 0);
            NodeOrNodeVectorWritable.dimVector = dimVector;

            u_vec = new float[dimVector];
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer st = new StringTokenizer(value.toString());
            int u = Integer.parseInt(st.nextToken());

            for(int i=0; i<dimVector; i++){
                u_vec[i] = Float.parseFloat(st.nextToken());
            }

            ok.set(u);
            nv.set(u, u_vec);
            context.write(ok, nv);

            nv.set(u);
            HashSet<Integer> included = new HashSet<>();
            included.add(u);
            for(int i=0; i<k; i++) {
                do ok.set(rand.nextInt(numVectors)); while (included.contains(ok.get()));
                included.add(ok.get());
                context.write(ok, nv);
            }
        }
    }

    public static class RandomGraphStep1Reducer extends Reducer<IntWritable, NodeOrNodeVectorWritable, IntWritable, NodeVectorWritable>{
        public int dimVector, k;

        NodeVectorWritable nv = new NodeVectorWritable();
        IntWritable v = new IntWritable();
        public float[] u_vec;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            dimVector = conf.getInt("dimVector",0);
            k = conf.getInt("k", 0);
            NodeVectorWritable.dimVector = dimVector;
            NodeOrNodeVectorWritable.dimVector = dimVector;

            u_vec = new float[dimVector];
        }

        @Override
        protected void reduce(IntWritable u, Iterable<NodeOrNodeVectorWritable> values, Context context) throws IOException, InterruptedException {

            ArrayList<Integer> nodes = new ArrayList<>();

            for(NodeOrNodeVectorWritable nonv : values){
                if(nonv.hasVector){
                    System.arraycopy(nonv.vector, 0, u_vec, 0, dimVector);
                }
                else{
                    nodes.add(nonv.id);
                }
            }

            nv.set(u.get(), u_vec, true, true, false);
            context.write(u, nv);

            for(int n : nodes){
                v.set(n);
                context.write(v, nv);
            }

        }
    }
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public void runRandomStep2(String inputPath, String outputPath) throws Exception{
        Job job = Job.getInstance(getConf(), "RandomGraphGen-step2");
        job.setJarByClass(NNDescent.class);

        job.setMapperClass(RandomGraphStep2Mapper.class);
        job.setReducerClass(RandomGraphStep2Reducer.class);


        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(NodeVectorWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(KNNItemWritable.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(false);
    }

    public static class RandomGraphStep2Mapper extends Mapper<IntWritable, NodeVectorWritable, IntWritable, NodeVectorWritable>{
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int dimVector = conf.getInt("dimVector",0);
            NodeVectorWritable.dimVector = dimVector;
        }

        @Override
        protected void map(IntWritable key, NodeVectorWritable value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static class RandomGraphStep2Reducer extends Reducer<IntWritable, NodeVectorWritable, IntWritable, KNNItemWritable>{
        public int dimVector, k;

        KNNItemWritable knn = new KNNItemWritable();

        @Override
        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            dimVector = conf.getInt("dimVector",0);
            k = conf.getInt("k", 0);
            NodeVectorWritable.dimVector = dimVector;
            KNNItemWritable.numNeighbors = k;
            KNNItemWritable.dimVector = dimVector;
            knn.init();
        }

        @Override
        protected void reduce(IntWritable key, Iterable<NodeVectorWritable> values, Context context) throws IOException, InterruptedException {
        	
            int u = key.get();
            int i=0;
            for(NodeVectorWritable nv : values){
                if(nv.id == u){
                    knn.id = nv.id;
                    System.arraycopy(nv.vector, 0, knn.vector, 0, dimVector);
                }
                else{
                    knn.neighbors[i] = nv.id;
                    knn.flag_new[i] = nv.flag_new;
                    System.arraycopy(nv.vector, 0, knn.vectors[i], 0, dimVector);
                    i++;
                }
            }

            assert i == k: "i("+i+") should be the same as k("+k+")";

            for(i=0; i<k; i++){
                knn.distances[i] = Distances.l2(knn.vector, knn.vectors[i]);
            }

            context.write(key, knn);
        }
    }
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public void run_step1(String inputPath, String outputPath, int round) throws Exception {

        Job job = Job.getInstance(getConf(), "NNDescent-step1-r"+round);
        job.setJarByClass(NNDescent.class);

        job.setMapperClass(Step1Mapper.class);
        job.setReducerClass(Step1Reducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(NodeVectorWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(NNItemWritable.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        MultipleOutputs.addNamedOutput(job, "unsampled", SequenceFileOutputFormat.class, IntWritable.class, NodeVectorWritable.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(false);

        mor = job.getCounters().findCounter(TaskCounter.MAP_OUTPUT_RECORDS).getValue();
        mob = job.getCounters().findCounter(TaskCounter.MAP_OUTPUT_BYTES).getValue();
    }

    public static class Step1Mapper extends Mapper<IntWritable, KNNItemWritable, IntWritable, NodeVectorWritable> {

        int dimVector, k, sampleSize;
        NodeVectorWritable nv_v = new NodeVectorWritable();
        NodeVectorWritable nv_u = new NodeVectorWritable();
        IntWritable u = new IntWritable();
        IntWritable v = new IntWritable();

        ArrayList<Integer> id_new_sample = new ArrayList<>();
        ArrayList<Integer> id_old = new ArrayList<>();

        MultipleOutputs<IntWritable, NodeVectorWritable> mos;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            dimVector = conf.getInt("dimVector",0);
            k = conf.getInt("k", 0);
            sampleSize = conf.getInt("sampleSize", 0);
            KNNItemWritable.numNeighbors = k;
            KNNItemWritable.dimVector = dimVector;
            NNItemWritable.dimVector = dimVector;
            NodeVectorWritable.dimVector = dimVector;
            mos = new MultipleOutputs(context);
            super.setup(context);
        }

        @Override
        protected void map(IntWritable key, KNNItemWritable value, Context context) throws IOException, InterruptedException {
        	
            u.set(value.id);
            nv_u.set(value.id, value.vector, false, true, false);
            context.write(u, nv_u);

            id_new_sample.clear();
            id_old.clear();

            for(int i=0; i<k; i++) {
                if (value.flag_new[i])
                    id_new_sample.add(i);
                else
                    id_old.add(i);
            }

            //sampling
            Collections.shuffle(id_new_sample);
            int size = id_new_sample.size();
            while(size-->sampleSize) {
                int i = id_new_sample.get(size);

                nv_v.set(value.neighbors[i], value.vectors[i], true, false, false);
//                context.write(u, nv_v);

                mos.write("unsampled", u, nv_v, "unsampled/part");

                id_new_sample.remove(size);
            }


            nv_u.flag_new = false;
            for(int i : id_old){
                v.set(value.neighbors[i]);
                context.write(v, nv_u);

                nv_v.set(value.neighbors[i], value.vectors[i], false, false, true);
                context.write(u, nv_v);
            }

            nv_u.flag_new = true;
            for(int i : id_new_sample){
                v.set(value.neighbors[i]);
                context.write(v, nv_u);

                nv_v.set(value.neighbors[i], value.vectors[i], true, false, true);
                context.write(u, nv_v);
            }
        }

        @Override
        protected void cleanup(Mapper<IntWritable, KNNItemWritable, IntWritable, NodeVectorWritable>.Context context) throws IOException, InterruptedException {
            mos.close();
        }
    }

    public static class Step1Reducer extends Reducer<IntWritable, NodeVectorWritable, IntWritable, NNItemWritable> {
        public int dimVector, k, sampleSize;

        NNItemWritable nn = new NNItemWritable();

        ArrayList<NodeVectorWritable> reverse_new = new ArrayList<>();
        ArrayList<NodeVectorWritable> reverse_old = new ArrayList<>();


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            dimVector = conf.getInt("dimVector",0);
            k = conf.getInt("k", 0);
            sampleSize = conf.getInt("sampleSize", 0);
            NNItemWritable.dimVector = dimVector;
            NodeVectorWritable.dimVector = dimVector;
            nn.initialize();
        }

        @Override
        protected void reduce(IntWritable key, Iterable<NodeVectorWritable> values, Context context) throws IOException, InterruptedException {
            int u = key.get();

            nn.initialize();

            reverse_new.clear();
            reverse_old.clear();

            for(NodeVectorWritable nv : values){
                if(nv.id == u) {
                    nn.setVector(nv.id, nv.vector);
                }
                else {
                    if(nv.flag_reverse){
                        if(nv.flag_new)
                            reverse_new.add(new NodeVectorWritable(nv.id, nv.vector.clone(), true, true, true));
                        else
                            reverse_old.add(new NodeVectorWritable(nv.id, nv.vector.clone(), false, true, false));
                    }
                    // nn 에는 old B(XXO), new sam B(OXO), new unsam B(OXX) 추가
                    else{
                        nn.add(nv.id, nv.vector, nv.flag_new, false, nv.flag_sample);
                    }
                }
            }

            sample(reverse_new, sampleSize);
            sample(reverse_old, sampleSize);

            // nn 에 new sam R(OOO) 추가
            for(NodeVectorWritable nv : reverse_new){
                nn.add(nv.id, nv.vector, true, true, true);
            }
            // nn 에 old sam R(XOO) 추가
            for(NodeVectorWritable nv : reverse_old){
                nn.add(nv.id, nv.vector, false, true, true);
            }

            context.write(key, nn);
        }

        public void sample(ArrayList<?> nn, int sampleSize){
            Collections.shuffle(nn);
            int size = nn.size();
            while(size-->sampleSize)
                nn.remove(size);
        }
    }
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public Counters run_step2(String inputPath, String unsamplePath, boolean unsam, String outputPath, int round) throws Exception {

        Job job = Job.getInstance(getConf(), "NNDescent-step2-r"+round);
        job.setJarByClass(NNDescent.class);

        job.setMapperClass(Step2Mapper.class);
        job.setReducerClass(Step2Reducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(NodeVectorWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(KNNItemWritable.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        MultipleInputs.addInputPath(job, new Path(inputPath), SequenceFileInputFormat.class, Step2Mapper.class);
        if(unsam) MultipleInputs.addInputPath(job, new Path(unsamplePath), SequenceFileInputFormat.class, Step2UnsampleMapper.class);
//        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(false);

        mor = job.getCounters().findCounter(TaskCounter.MAP_OUTPUT_RECORDS).getValue();
        mob = job.getCounters().findCounter(TaskCounter.MAP_OUTPUT_BYTES).getValue();

        return new Counters(job.getCounters().findCounter(NNDCounters.NUM_CHANGES).getValue(), 0, job.getCounters().findCounter(NNDCounters.NUM_NODES).getValue(), job.getCounters().findCounter(NNDCounters.NUM_EDGES).getValue());
    }

    public static class Step2UnsampleMapper extends Mapper<IntWritable, NodeVectorWritable, IntWritable, NodeVectorWritable>{

        @Override
        protected void setup(Mapper<IntWritable, NodeVectorWritable, IntWritable, NodeVectorWritable>.Context context) throws IOException, InterruptedException {
            NodeVectorWritable.dimVector = context.getConfiguration().getInt("dimVector",0);
        }

        @Override
        protected void map(IntWritable key, NodeVectorWritable value, Mapper<IntWritable, NodeVectorWritable, IntWritable, NodeVectorWritable>.Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }


    public static class Step2Mapper extends Mapper<IntWritable, NNItemWritable, IntWritable, NodeVectorWritable> {
        public int dimVector, k;

        NodeVectorWritable nvi = new NodeVectorWritable();
        NodeVectorWritable nvj = new NodeVectorWritable();
        IntWritable vi = new IntWritable();
        IntWritable vj = new IntWritable();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            dimVector = conf.getInt("dimVector",0);
            k = conf.getInt("k", 0);
            NNItemWritable.dimVector = dimVector;
            NodeVectorWritable.dimVector = dimVector;
        }

        @Override
        protected void map(IntWritable key, NNItemWritable value, Context context) throws IOException, InterruptedException {

            vi.set(value.id);
            nvi.set(value.id, value.vector, false, false, false);
            context.write(vi, nvi);
            
            for(int i=0; i < value.numNeighbors; i++) {
                if(!value.flag_reverse.get(i)) {
                    // Note: We use the reverse flag to mark the current neighbor temporarily.
                    nvj.set(value.neighbors.get(i), value.vectors.get(i), value.flag_new.get(i), false, value.flag_sample.get(i));
                    context.write(vi, nvj);
                }
            }

            for(int i=0; i < value.numNeighbors; i++){
                if(!value.flag_sample.get(i)) continue;

                vi.set(value.neighbors.get(i));
                nvi.set(vi.get(), value.vectors.get(i), value.flag_new.get(i), true, value.flag_sample.get(i));
                for(int j=i+1; j < value.numNeighbors; j++){
                    if(!value.flag_sample.get(j)) continue;

                    vj.set(value.neighbors.get(j));
                    nvj.set(vj.get(), value.vectors.get(j), value.flag_new.get(j), true, value.flag_sample.get(j));

                    if((nvi.flag_new || nvj.flag_new) && (vi.get() != vj.get())){
                        context.write(vi, nvj);
                        context.write(vj, nvi);
                    }
                }
            }
        }
    }

    public static class Step2Reducer extends Reducer<IntWritable, NodeVectorWritable, IntWritable, KNNItemWritable>{
        public int dimVector, k;

        KNNItemWritable knn = new KNNItemWritable();
        HashSet<Integer> currentIdSet = new HashSet<>();


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            dimVector = conf.getInt("dimVector",0);
            k = conf.getInt("k", 0);
            KNNItemWritable.numNeighbors = k;
            KNNItemWritable.dimVector = dimVector;
            NodeVectorWritable.dimVector = dimVector;
            knn.init();
        }

        @Override
        protected void reduce(IntWritable key, Iterable<NodeVectorWritable> values, Context context) throws IOException, InterruptedException {

            int n = key.get();

            ArrayList<NodeVectorDistance> nns = new ArrayList<>();
            
            currentIdSet.clear();
            
            // B(v) + nn(v)
            for(NodeVectorWritable nv : values){
                if(nv.id == n) {
                    knn.id = nv.id;
                    System.arraycopy(nv.vector, 0, knn.vector, 0, dimVector);
                }
                else {
                    if(!nv.flag_reverse) currentIdSet.add(nv.id);
                    boolean is_new = nv.flag_reverse || (nv.flag_new && !nv.flag_sample);
                    nns.add(new NodeVectorDistance(nv.id, nv.vector.clone(), Float.POSITIVE_INFINITY, is_new));
                }
            }
            
            for(NodeVectorDistance nvd : nns){
                nvd.dist = Distances.l2(knn.vector, nvd.vector);
            }

            PriorityQueue<NodeVectorDistance> pq = new PriorityQueue<>(nns);

            int prevId = -1;
            for(int i=0; i<k; i++){
                NodeVectorDistance x;
                do x = pq.poll(); while(x.id == prevId);
                prevId = x.id;

                knn.neighbors[i] = x.id;
                knn.distances[i] = x.dist;
                knn.flag_new[i] = x.flag_new;
                System.arraycopy(x.vector, 0, knn.vectors[i], 0, dimVector);

                if(i == k-1)
                {
                    while(x != null && x.id == prevId) x = pq.poll();
                }
            }

            int numChanges = 0;
            for(int id : knn.neighbors) if(!currentIdSet.contains(id)) numChanges++;

            if(numChanges == 0)
            {
                context.getCounter(NNDCounters.NUM_NODES).increment(1);
                if(pq.size() == 0) context.getCounter(NNDCounters.NUM_EDGES).increment(1);
            }

            context.getCounter(NNDCounters.NUM_CHANGES).increment(numChanges);

            context.write(key, knn);
        }
    }
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public void write_result(String inputPath, String outputPath) throws Exception
    {
        Job job = Job.getInstance(getConf(), "Final_Result");
        job.setJarByClass(NNDescent.class);

        job.setMapperClass(ResultMapper.class);
        job.setReducerClass(ResultReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(true);
    }

    public static class ResultMapper extends Mapper<IntWritable, KNNItemWritable, IntWritable, IntWritable>
    {
        IntWritable v = new IntWritable(-1);

        @Override
        protected void setup(Mapper<IntWritable, KNNItemWritable, IntWritable, IntWritable>.Context context)
                throws IOException, InterruptedException {

            KNNItemWritable.dimVector = context.getConfiguration().getInt("dimVector", 0);
            KNNItemWritable.numNeighbors = context.getConfiguration().getInt("k", 0);
        }

        @Override
        protected void map(IntWritable key, KNNItemWritable value,
                           Mapper<IntWritable, KNNItemWritable, IntWritable, IntWritable>.Context context)
                throws IOException, InterruptedException {

            for(int val : value.neighbors)
            {
                v.set(val);
                context.write(key, v);
            }
        }
    }

    public static class ResultReducer extends Reducer<IntWritable, IntWritable, IntWritable, Text>
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
