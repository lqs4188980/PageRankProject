import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class OutputKey implements WritableComparable<OutputKey>{
	private Text page; 
	private DoubleWritable rank;
	
	public void Set(Text page, DoubleWritable rank){
		this.page = page;
		this.rank = rank;
	}

	public OutputKey() {
		// TODO Auto-generated constructor stub
		Set(new Text(), new DoubleWritable());
	}
	public OutputKey(String page, double rank) {
		Set(new Text(page), new DoubleWritable(rank));
	}
	
	public double getRank() {
		return rank.get(); 
	}
	public String getPage() {
		return page.toString(); 
	}

	@Override
	public void write(DataOutput out) throws IOException {
		page.write(out);
		rank.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		page.readFields(in);
		rank.readFields(in);
		//Serialization | 97
	}


	@Override
	public String toString() {
		return page.toString() + "\t" + rank; 
	}

	@Override
	public int compareTo(OutputKey p) {
		int cmp = rank.compareTo(p.rank);
		if (cmp != 0) {
			return -1 * cmp;
		}
		return page.compareTo(p.page);
	}
	
}
