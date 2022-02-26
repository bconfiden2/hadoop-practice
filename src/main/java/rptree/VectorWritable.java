package rptree;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class VectorWritable implements Writable {

    public static int dimVector = 0;

    public float[] vector;
    public String path;

    public VectorWritable(){}

    public VectorWritable(int dim) {
        vector = new float[dimVector];
        path = "0";
    }

    public VectorWritable(float[] vector, String path){
        set(vector, path);
    }

    public void set(float[] vector, String path){
        this.vector = vector;
        this.path = path;
    }

    public void update(float[] src) {
        System.arraycopy(src, 0, vector, 0, dimVector);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for(int i = 0 ; i < dimVector ; i++) {
            sb.append(vector[i]);
            sb.append(' ');
        }
        return sb.toString() + path;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        for (int i = 0; i < dimVector; i++)
            out.writeFloat(vector[i]);
        out.writeBytes(path);
//        out.writeChars(path);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        if (vector == null)
            vector = new float[dimVector];

        for (int i = 0; i < dimVector; i++)
            vector[i] = in.readFloat();

        path = in.readLine().trim();
    }
}
