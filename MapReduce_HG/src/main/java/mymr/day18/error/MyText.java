package mymr.day18.error;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class MyText implements WritableComparable<MyText> {
	String str = "";
	public MyText(String str) {
		this.str = str;
	}
	public MyText() {
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(str);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		str = in.readUTF();
	}
	@Override
	public String toString() {
		return  str;
	}
	@Override
	public int compareTo(MyText o) {
		return this.str.compareTo(o.str);
	}
	

}
