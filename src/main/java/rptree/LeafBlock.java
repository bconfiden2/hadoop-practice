package rptree;

public class LeafBlock {
    public static int dimVector;
    public static int maxBlockSize;

    public int blockSize;
    public int[] ids;
    public float[][] vectors;

    public LeafBlock(){

    }

    public void initialize() {
        blockSize = 0;
        ids = new int[maxBlockSize];
        vectors = new float[maxBlockSize][dimVector];
    }

    public void reset()
    {
        blockSize = 0;
    }

    public void add(int id, float[] vector) {
        ids[blockSize] = id;
        System.arraycopy(vector, 0, vectors[blockSize], 0, dimVector);
        blockSize++;
    }
}
