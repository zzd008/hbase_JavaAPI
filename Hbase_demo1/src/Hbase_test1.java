import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

//实现使用列族：列限定符来插入数据
public class Hbase_test1 {
	/**
	 * @param args
	 * @throws IOException
	 */
	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		Configuration conf=null;
		Connection con=null;
		Admin admin=null;
		try {
			conf=HBaseConfiguration.create();
			conf.set("hbase.rootdir", "hdfs://Test:9000/hbase");
			con=ConnectionFactory.createConnection(conf);
			admin=con.getAdmin();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		Scanner sca=new Scanner(System.in);
		System.out.println("*****插入数据*****");
		System.out.println("输入表名：");
		String tableName=sca.next();
		System.out.println("输入行号:");
		String row=sca.next();
		System.out.println("输入列：");
		String col=sca.next();
		System.out.println("请输入值：");
		String value=sca.next();
		
		TableName tn=TableName.valueOf(tableName);
		Table table=con.getTable(tn);
		 
		String[] split=null;
		if(!admin.tableExists(tn)){
			System.out.println("该表不存在！");
		}else{
			Put put=new Put(row.getBytes());
			//如果列为info:Name则报错，因为列族不匹配　如果为info则直接把数据插在该列族下（列族下可以没有列）
			//put.add(col.getBytes(),null,value.getBytes()); 
			split = col.split(":");//所以就把info:Name分割一下啦
			put.add(split[0].getBytes(), split[1].getBytes(), value.getBytes());
			//put.addColumn(col.getBytes(), null, value.getBytes());　这个方法和add()当大作用差不多　add方法已过期
			table.put(put);
			System.out.println("数据插入成功！");
		}
		
		//将插入的数据查询出来
		Get get=new Get(row.getBytes());
		get.addColumn(split[0].getBytes(), Bytes.toBytes(split[1]));
		Result result = table.get(get);
		System.out.println(new String(result.getValue(split[0].getBytes(), Bytes.toBytes(split[1]))));
		Cell[] cells = result.rawCells();
		for(Cell c:cells){
			System.out.println("*****"+table.getName()+"*****");
			System.out.print(new String(CellUtil.cloneRow(c))+"   ");
			System.out.print(new String(CellUtil.cloneFamily(c))+":");
			System.out.print(new String(CellUtil.cloneQualifier(c))+"   ");
			System.out.print(new String(CellUtil.cloneValue(c)));
		}
	}
}
