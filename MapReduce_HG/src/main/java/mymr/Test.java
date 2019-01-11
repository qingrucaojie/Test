package mymr;

import java.io.File;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IOUtils;

public class Test {
	public static void main(String[] args) {
		/*System.out.println("niha".matches("\\w{5,12}"));
		PrintStream out = System.out;
		System.out.println("======================---------");
//		out.close();*/	
		/*System.out.println(Integer.MIN_VALUE);
		System.out.println("abc" instanceof String);
		// my name is zhangsan
		new Test().getFile(new File("E:\\��ѧ�μ����ܣ�\\�����ݿμ�\\02.�����ݵڶ�����\\���ε�����6.0\\6��6.0�����ݶ�"));
		for (File file : filelList) {
			System.out.println(file.getPath().toString());
		}
		for (File file : emptyDirList) {
			System.out.println(file.getPath().toString());
		}*/
		
		/*System.out.println(~100);
		int a = 123;
		System.out.println(Integer.toBinaryString(a));*/
//		System.out.println(1_2_3);
		byte b = 0b10101;
	}
	/*static List<File> filelList = new ArrayList<>();
	static List<File> emptyDirList = new ArrayList<>();
	public void getFile(File file){
		if(file.isDirectory()){
			File[] listFiles = file.listFiles();
			
			if(listFiles.length == 0){
				emptyDirList.add(file);
			}else{
				for (int i = 0; i < listFiles.length; i++) {
					getFile(listFiles[i]);
				}	
			}
		}else{
			filelList.add(file);
		}
	}*/
}




















