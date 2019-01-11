package mymr.day18;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
//Writable的自定义类型不是所有地方都可以使用
/*
 * 有mapper阶段和reducer阶段的map()的key,不能用
 */
public class MyIntWritable implements Writable{

	int num1 = 0;
	
	// alt + shift + s 选择有参构造
	public MyIntWritable(int num1) {
		this.num1 = num1;
	}

	
	// alt + shift + s 选择无参构造
	public MyIntWritable() {
	}

	public int get() { return num1; }

	@Override
	//所传递的类型如何与磁盘、io、网络交互
	public void write(DataOutput out) throws IOException {
		//如何放入磁盘中数据的顺序  在下面read中就如何取出顺序一定不能变
		out.writeInt(num1);
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		//上面如何存入的数据顺序就如何取出,一定不能改变顺序
		num1 = in.readInt();
	}


	@Override
	public String toString() {
		return num1+"";
	}

	
	
}






