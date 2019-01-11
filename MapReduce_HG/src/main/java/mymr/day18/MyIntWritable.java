package mymr.day18;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
//Writable���Զ������Ͳ������еط�������ʹ��
/*
 * ��mapper�׶κ�reducer�׶ε�map()��key,������
 */
public class MyIntWritable implements Writable{

	int num1 = 0;
	
	// alt + shift + s ѡ���вι���
	public MyIntWritable(int num1) {
		this.num1 = num1;
	}

	
	// alt + shift + s ѡ���޲ι���
	public MyIntWritable() {
	}

	public int get() { return num1; }

	@Override
	//�����ݵ������������̡�io�����罻��
	public void write(DataOutput out) throws IOException {
		//��η�����������ݵ�˳��  ������read�о����ȡ��˳��һ�����ܱ�
		out.writeInt(num1);
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		//������δ��������˳������ȡ��,һ�����ܸı�˳��
		num1 = in.readInt();
	}


	@Override
	public String toString() {
		return num1+"";
	}

	
	
}






