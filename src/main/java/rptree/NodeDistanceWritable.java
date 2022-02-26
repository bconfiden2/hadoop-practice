package rptree;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class NodeDistanceWritable implements WritableComparable<NodeDistanceWritable> {

    public int id;
    public float distance;

    public NodeDistanceWritable() {}

    public NodeDistanceWritable(int id, float dist)
    {
        this.id = id;
        this.distance = dist;
    }

    public void set(int id, float dist)
    {
        this.id = id;
        this.distance = dist;
    }

    @Override
    public String toString() {
        return "NodeDistanceWritable{" +
                "id=" + id +
                ", distance=" + distance +
                '}';
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(id);
        dataOutput.writeFloat(distance);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        id = dataInput.readInt();
        distance = dataInput.readFloat();
    }

    @Override
    public int compareTo(NodeDistanceWritable other) {
        if(distance != other.distance)
            return Float.compare(distance, other.distance);
        else
            return Integer.compare(id, other.id);
    }
}
