package mymr.day19;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class SecondSortKey implements WritableComparable<SecondSortKey> {

	int firstNum = 0;
	int secondNum = 0;
	
	public void set(int firstNum, int secondNum) {
		this.firstNum = firstNum;
		this.secondNum = secondNum;
	}
	
	/**
	 * @param firstNum
	 * @param secondNum
	 */
	public SecondSortKey(int firstNum, int secondNum) {
		this.firstNum = firstNum;
		this.secondNum = secondNum;
	}
	

	public SecondSortKey() {
	}


	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(firstNum);
		out.writeInt(secondNum);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		firstNum = in.readInt();
		secondNum = in.readInt();
	}

	@Override
	/*
	 * 两列数据,第一列倒叙,如果第一列相同第二列正序
	 */
	public int compareTo(SecondSortKey o) {
		int first = o.firstNum - this.firstNum;
		if(first == 0){
			int second = this.secondNum - o.secondNum;
			return second;
		}
		return first;
	}


	@Override
	public String toString() {
		return firstNum + "     " + secondNum;
	}
	

}
