package nndescent;

public class Counters {
	public long changes;
	public long scans;
	public long nodes;
	public long edges;
	
	public Counters(long changes)
	{
		this.changes = changes;
	}
	
	public Counters(long changes, long scans)
	{
		this.changes = changes;
		this.scans = scans;
	}
	
	public Counters(long changes, long scans, long nodes)
	{
		this.changes = changes;
		this.scans = scans;
		this.nodes = nodes;
	}
	
	public Counters(long changes, long scans, long nodes, long edges)
	{
		this.changes = changes;
		this.scans = scans;
		this.nodes = nodes;
		this.edges = edges;
	}
}
