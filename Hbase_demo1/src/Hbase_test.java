import java.io.IOException;

import com.Hbase_Utils;


public class Hbase_test {
	public static void main(String[] args) throws IOException {
		//Hbase_Utils.createTable("Student",new String[]{"info","score"} );//创建表
		//Hbase_Utils.deleteTable("T1");//删除表
		//Hbase_Utils.listTables();//显示所有的表
		
		//插入一行完整的学生信息：info(姓名、性别)，score(数学成绩，java成绩)
		/*Hbase_Utils.insertData("Student", "row1", "info", "Name", "zzd");
		Hbase_Utils.insertData("Student", "row1", "info", "Age", "18");
		Hbase_Utils.insertData("Student", "row1", "score", "Math_gradge", "99");
		Hbase_Utils.insertData("Student", "row1", "score", "Java_gradge", "88");
		Hbase_Utils.insertData("Student", "row1", "score", "", "88"); 不同的插入方式
		Hbase_Utils.insertData("Student", "row1", "score", null, "88");*/
		
		//Hbase_Utils.insertData("Student", "row2", "info:name","name" ,"88");这样info:name不存在，会报错
		
		/*try {		//查询单元格数据
			Hbase_Utils.getData("Student", "row1", "info", "Name");
		} catch (Exception e) {
			System.out.println("你指定的单元格不存在！");
			e.printStackTrace();
		}*/
		
		//查询某一行所有数据
		//Hbase_Utils.getRowData("Student", "row1");
		
		//删除数据
		//Hbase_Utils.deleteData("Student", "row1", "info", "Java_gradge");
		//Hbase_Utils.deleteData("Student", "row1", "info", "Math_gradge");
	
		//增加列族
		//Hbase_Utils.addColum("Student", "course");
		
		//插入多行数据
		/*Hbase_Utils.insertdatas(
				"Student", new String[]{"row10","row11", "row12"}, new String[]{"info","score","course"}, new String[]{"Name","Java_gradge","Type"},new String[]{"zhangtao","100","PE"}
		);*/
		
		//扫描表
		//Hbase_Utils.scanData();
		
		//根据时间戳查询数据
		Hbase_Utils.scanDataByTimeStamp();
	}
}
