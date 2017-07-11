package com;
//javaAPI对应着Hbase shell命令
import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.generated.master.table_jsp;
import org.apache.hadoop.hbase.util.Bytes;
//要将habse/lib下所有jar包都导进来(但是很奇怪将它们装在用户jar包user libary里会有错？？)，而且不能到导入hdfs的，避免版本冲突
public class Hbase_Utils {
	private static Configuration config;//配置文件对象
	private static Connection conn;//连接对象，用于连接数据库
	private static Admin admin;//用于管理数据库表的信息：创建表、删除表、添加列族。。。。
	//HbaseAdmin admin
	
	//建立连接
	public static void init(){
		config=HBaseConfiguration.create();//获得Configuration配置对象
		config.set("hbase.rootdir", "hdfs://Test:9000/hbase");//设置hbse数据库所在位置
		try{
			conn=ConnectionFactory.createConnection(config);//获取一个数据库连接
			admin=conn.getAdmin();//从连接中获取管理对象
			//admin=new HBaseAdmin(config); 也可以这样
			/*if(admin!=null&&conn!=null){
				System.out.println("数据库连接正常!");
			}*/
		}catch(IOException  e){
			System.out.println("数据库连接异常！");
			e.printStackTrace();
		}
	}
	
	//关闭连接
	public static void close(){
		try {
			if(admin!=null){
				admin.close();
			}
			if(conn!=null){
				conn.close();
			}
			//System.out.println("数据库关闭正常！");
		} catch (IOException e) {
			System.out.println("数据库关闭异常！");
			e.printStackTrace();
		}
	}
	
	//创建表 注意各个方法的参数！ 表的每一个部分都封装成了对象！
	public static void createTable(String tableName,String []colFamily) throws IOException{//表名、列族
		init();//首先先要建立连接
		TableName tn=TableName.valueOf(tableName);//表名对象　
		//判断表是否存在
		if(admin.tableExists(tn)){//Admin类的这个方法的参数是一个TableName对象，而HbaseAdmin类的这个方法的参数是字符串就可以
			System.out.println("该表已经存在！");
			//admin.disableTable(tn); //使表失效
			//admin.deleteTable(tn);  //删除表
		}else{
			//HTableDescriptor对象：（描述）管理表的详细信息如增加列族、删除列族等
			HTableDescriptor hTableDescriptor=new HTableDescriptor(tableName);//参数是tbName也可以
			//遍历，给表添加列族
			for(String str:colFamily){
				HColumnDescriptor column=new HColumnDescriptor(str);//列族对象
				hTableDescriptor.addFamily(column);//添加列族，参数为列族对象
			}
			//添加列族后，就可以创建表了
			admin.createTable(hTableDescriptor);
			//检查表是否可用
			Boolean isAviliable=admin.isTableAvailable(tn);
			System.out.println("表"+tableName+"是否可用："+isAviliable);
			System.out.println("表"+tableName+"创建成功！");
		}
		close();//关闭链接
	}
	
	//删除表
	public static void deleteTable(String tableName) throws IOException{
		init();
		TableName tn=TableName.valueOf(tableName);
		if(!admin.tableExists(tn)){
			System.out.println("该表不存在！");
		}else{
			admin.disableTable(tn);//先失效再删除
			admin.deleteTable(tn);
			System.out.println("表"+tableName+"已删除");
		}
		close();//关闭链接
	}
	
	//查看已有表信息
	public static void listTables() throws IOException{
		init();
		//方法一
		HTableDescriptor[] tables = admin.listTables();//获取table集合
		for(HTableDescriptor t:tables){
			System.out.println(t.getNameAsString());//输出表名
		}
		//方法二
		TableName[] tables1 = admin.listTableNames();
		for(TableName t:tables1){
			System.out.println(t);//不用toString()
		}
		//根据表名获取表的信息
		HTableDescriptor tableDescriptor = admin.getTableDescriptor(TableName.valueOf("Student"));
		System.out.println(tableDescriptor);
		close();//关闭链接
	}
	
	//插入单行数据
	public static void insertData(String tableName,String row,String colFamily,String col,String value) throws IOException{
		init();
		//Table table=conn.getTable(TableName.valueOf(tableName));//Table对象作用同Ｈtable 是它的父类
		HTable table=new HTable(config, tableName);//HTable对象负责与Hbase通信，如对表中数据的增加、删、查询、判断数据是否存在等　这方法过期了。。。
		Put put=new Put(row.getBytes());//Put对象用于对单元格插入数据、判断值是否存在等等　构造参数是行　
		//因为Ｈbase中数据都是	以字节数组方式存在的　所以要	getBytes() 也可以用包装类Bytes.toBytes()
		//put.add(colFamily.getBytes(), col.getBytes(), value.getBytes());//向（put对象）单元格中插入值 
		//add方法已过期，用下面的：
		if(col!=null){
			put.addColumn(colFamily.getBytes(), col.getBytes(), value.getBytes());
		}else{//如果没指定列限定符，就直接放在列族下
			put.addColumn(colFamily.getBytes(), null, value.getBytes());
			//插入数据时只能往某一行的一个列族下的一个列下插入数据
			//put.add(family, qualifier, value); put中可以放入好多列的数据　然后一起插入
		}
		table.put(put);//put方法用于插入数据
		System.out.println("数据插入成功！");
		table.close();//关闭资源
		close();//关闭链接
	}
	
	//获取数据(某一个单元格)
	public static void getData(String tableName,String row,String colFamily,String col) throws IOException{
		init();
		Table table=conn.getTable(TableName.valueOf(tableName));
		Get get=new Get(row.getBytes());//Get对象作用同Put
		get.addColumn(colFamily.getBytes(), col.getBytes());//定位到要查找的单元格
		Result result=table.get(get);//查询出的数据结果集合，还需要格式化输出才行
		//根据列族、列限定符来获取对应单元格最新的数据(先得到字节数组，然后再格式化成String字符串)
		//System.out.println(new String(result.getValue(colFamily.getBytes(), col.getBytes())));
		//格式化输出数据
		showCell(result);//这里的result只包含一个单元格，没必要用这个方法了
		table.close();//关闭资源
		close();//关闭链接
	}
	
	//获取数据(某一行)
		public static void getRowData(String tableName,String row) throws IOException{
			init();
			Table table=conn.getTable(TableName.valueOf(tableName));
			Get get=new Get(row.getBytes());//Get对象作用同Put
			Result result=table.get(get);//查询出的数据结果集合（查询一行）
			for(Cell c:result.rawCells()){
				System.out.print("行健："+new String(CellUtil.cloneRow(c))+"\t");
				System.out.print("列族："+new String(CellUtil.cloneFamily(c))+"\t");
				System.out.print("列："+new String(CellUtil.cloneQualifier(c))+"\t");
				System.out.print("时间戳："+c.getTimestamp()+"\t");
				System.out.println("值："+new String(CellUtil.cloneValue(c)));
			}
			table.close();//关闭资源
			close();//关闭链接
		}
		
	//扫描数据
	public static void scanData() throws IOException{
		init();
		HTable table=new HTable(config,TableName.valueOf("Student"));
		//获取扫描对象
		Scan sca=new Scan();
		sca.addColumn("info".getBytes(), "Name".getBytes());//添加要扫描的列族：列
		sca.addFamily("score".getBytes());//添加列族(所有列)
		sca.setStartRow("row1".getBytes());//设置扫描的起始行
		sca.setStopRow("row5".getBytes());//设置扫描的结束行
		//一个ResultScanner(容器)包含多个result(一行)，一个Result(一行)包含多个cell(单元格)
		//获取result容器
		ResultScanner scanner = table.getScanner(sca);
		//遍历
		for(Result result:scanner){
			//获取单元格
			Cell cells[]=result.rawCells();
			//遍历每一个单元格
			for(Cell c:cells){
				System.out.print("行健："+new String(CellUtil.cloneRow(c))+"   ");
				System.out.print("列族："+new String(CellUtil.cloneFamily(c))+"   ");
				System.out.print("列："+new String(CellUtil.cloneQualifier(c))+"   ");
				System.out.print("时间戳："+c.getTimestamp()+"   ");
				System.out.println("值："+new String(CellUtil.cloneValue(c))+"   ");
				System.out.println("-----------------------------------------------------------");
			}
		}
		table.close();
		close();
		
	}
	
	//显示指定时间戳范围内的数据
	public static void scanDataByTimeStamp() throws IOException{
		init();
		HTable table=new HTable(config, TableName.valueOf("Student"));
		Scan sca=new Scan();
		//指定时间戳查询
		//sca.setTimeStamp(Long.parseLong("1496918532230"));
		//设置时间戳范围
		sca.setTimeRange(Long.parseLong("1431234567841"), NumberUtils.toLong("1514564789415"));//long类型转化的两种的方式
		ResultScanner scanner=table.getScanner(sca);
		System.out.println("*****"+table.getTableName()+"*****");
		for(Result result:scanner){
			Cell cells[]=result.rawCells();
			for(Cell c:cells){//byte数组转化为字符串的两种方式
				System.out.print("行健："+Bytes.toString(CellUtil.cloneRow(c))+"   ");
				System.out.print("列族：列:"+new String(CellUtil.cloneFamily(c))+":"+new String(CellUtil.cloneQualifier(c))+"   ");
				System.out.print("时间戳："+c.getTimestamp()+"   ");
				System.out.println("值："+new String(CellUtil.cloneValue(c))+"   ");
				System.out.println("-----------------------------------------------------------");
			}
		}
		close();
	}
	
	//删除数据
	public static void deleteData(String tableName,String row,String colFamily,String col) throws IOException{
		init();
		Table table=conn.getTable(TableName.valueOf(tableName));
		Delete del=new Delete(row.getBytes());//Delete对象用于删除单元格
		del.addColumn(colFamily.getBytes(), col.getBytes());//根据单元格去删除　可以不添加单元格，即删除整行数据 
		table.delete(del);
		System.out.println("数据删除成功！");
		table.close();//关闭资源
		close();//关闭链接
	}
	
	//格式化输出 
	public static void showCell(Result result){
		Cell []cells=result.rawCells();//result是一个结果集，里面可能包含很多个cell(单元格) result.rawCells()得到所有单元格
		for(Cell cell:cells){
			//System.out.println("行健："+new String(cell.getRow())); getRow()方法已过期，不推荐使用
			System.out.println("行健："+new String(CellUtil.cloneRow(cell)));//CellUtil：cell对象的工具类
			System.out.println("时间戳："+cell.getTimestamp());
			System.out.println("列族："+new String(CellUtil.cloneFamily(cell)));
			System.out.println("列："+new String(CellUtil.cloneQualifier(cell)));
			System.out.println("值："+new String(CellUtil.cloneValue(cell)));
		}
	}
	
	//增加列族
	public static void addColum(String tableName,String column) throws TableNotFoundException, IOException{
		init();
		//获取表名对象
		TableName tn = TableName.valueOf(tableName);
		//根据表名获取表描述对象
		HTableDescriptor htd=admin.getTableDescriptor(tn);
		//列族对象
		HColumnDescriptor hcd=new HColumnDescriptor(column.getBytes());
		//判断类族是否存在
		if(htd.hasFamily(column.getBytes())){
			System.out.println("列族已经存在！");
		}else{
			//添加列族
			htd.addFamily(hcd);  //删除列族用removeFamily()方法
			//先使表失效，再更改表的模式，最后使表生效
			admin.disableTable(tn);
			admin.modifyTable(tn, htd);
			admin.enableTable(tn);
			System.out.println("列族添加成功！");
		}
		close();
	}
	
	//插入多行数据 同理可以查询多条数据Htable.get(ArrayList<Get>)
	public static void insertdatas(String tableName,String []rows,String []columnFamily,String []col,String []values) throws IOException{
		init();
		HTable table=new HTable(config, tableName);
		//使用集合，将put对象放到集合中，然后依次插入多行数据
		ArrayList<Put> list=new ArrayList<Put>();
		for(int i=0;i<rows.length;i++){
			Put put=new Put(Bytes.toBytes(rows[i]));
			put.addColumn(columnFamily[i].getBytes(), col[i].getBytes(), values[i].getBytes());
			list.add(put);
		}
		for(Put put:list){
			table.put(put);
		}
		System.out.println("多行数据已经插入！");
		close();
	}
	
	
}
