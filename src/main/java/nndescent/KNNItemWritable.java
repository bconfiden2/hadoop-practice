package nndescent;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class KNNItemWritable implements Writable {

    public int id;
    public static int numNeighbors = 0;
    public static int dimVector = 0;

    public float[] vector;

    public int[] neighbors;
    public float[] distances;
    public float[][] vectors;
    public boolean[] flag_new;

    public void init(){

        if(vector == null){
            vector = new float[dimVector];
        }

        if(neighbors == null){
            this.neighbors = new int[numNeighbors];
            this.distances = new float[numNeighbors];
            this.vectors = new float[numNeighbors][dimVector];
            this.flag_new = new boolean[numNeighbors];
        }
    }


    @Override
    public void write(DataOutput out) throws IOException {

        out.writeInt(id);

        for(float i : vector)
            out.writeFloat(i);

        for(int i = 0; i < numNeighbors; i++){
            out.writeInt(neighbors[i]);
            out.writeBoolean(flag_new[i]);
            out.writeFloat(distances[i]);
            for(int j = 0; j < dimVector; j++)
                out.writeFloat(vectors[i][j]);
        }

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readInt();

        init();

        for(int i = 0; i < dimVector; i++)
            this.vector[i] = in.readFloat();

        for(int i = 0; i < numNeighbors; i++){
            neighbors[i] = in.readInt();
            flag_new[i] = in.readBoolean();
            distances[i] = in.readFloat();
            for(int j = 0; j < dimVector; j++)
                vectors[i][j] = in.readFloat();
        }
    }

    @Override
    public String toString() {
        return "KNNItemWritable{" +
                "id=" + id +
                ", neighbors=" + Arrays.toString(neighbors) +
                ", flag_new=" + Arrays.toString(flag_new) +
                ", distances=" + Arrays.toString(distances) +
                ", vector=" + Arrays.toString(vector) +
                ", vectors=" + Arrays.deepToString(vectors) +
                '}';
    }
}
