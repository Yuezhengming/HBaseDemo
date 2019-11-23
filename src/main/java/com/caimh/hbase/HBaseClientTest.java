package com.caimh.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by caimh on 2019/10/16.
 */
public class HBaseClientTest {

    private static Connection conn = null;
    private static Admin admin = null;

    /**
     * 初始化（configuration,Connection,HBaseAdmin）
     *
     * @throws IOException
     */
    @Before
    public void init() throws IOException {
        //获取Configure对象，使用HBaseConfiguration的单例方法实例化
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "master-node:2181,slave-node1:2181,slave-node2:2181");

        //在HBase中管理、访问表需要先创建HBaseAdmin对象
        conn = ConnectionFactory.createConnection(conf);
        admin = conn.getAdmin();
    }

    /**
     * 释放资源
     */
    @After
    public void close() {
        if (conn != null) {
            try {
                conn.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        if (admin != null) {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 判断表是否存在
     *
     * @throws IOException
     */
    @Test
    public void testTableExsit() throws IOException {
        System.out.println(admin.tableExists(TableName.valueOf("user_info")));
        System.out.println(admin.tableExists(TableName.valueOf("staff")));
    }

    /**
     * 创建表
     */
    @Test
    public void testCreateTable() throws IOException {
        //创建表描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf("staff"));
        //创建列描述器
        HColumnDescriptor cf = new HColumnDescriptor("cf1");
        hTableDescriptor.addFamily(cf);
        //创建表
        admin.createTable(hTableDescriptor);
    }

    /**
     * 删除表
     */
    @Test
    public void testDeleteTable() throws IOException {
        //1.使表不可用
        admin.disableTable(TableName.valueOf("staff"));
        //2.删除表
        admin.deleteTable(TableName.valueOf("staff"));
    }

    /**
     * 插入数据
     */
    @Test
    public void testPutData() throws IOException {

        //1.获取表对象
        Table table = conn.getTable(TableName.valueOf("user_info"));

        //2.构造Put对象(封装插入数据信息，行键，)
        String rowkey = "001";//行键
        String cf = "f2";//列族名
        String cn = "username";//列名
        String value = "caimh";//列值

        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn), Bytes.toBytes(value));

        //3.插入数据
        table.put(put);
    }

    /**
     * 查询一条数据
     */
    @Test
    public void testGetData() throws IOException {
        String tableName = "user_info";
        String rowkey = "001";
        getData(tableName, rowkey);
    }

    /**
     * 获取一行数据（指定列族:列）
     *
     * @throws IOException
     */
    @Test
    public void testGetDataByCN() throws IOException {
        String tableName = "user_info";
        String rowkey = "001";
        String cf = "f1";
        String cn = "name";

        getDataByCN(tableName, rowkey, cf, cn);
    }

    private void getDataByCN(String tableName, String rowkey, String cf, String cn) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));

        Get get = new Get(Bytes.toBytes(rowkey));
        get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn));
        //get.setMaxVersions(3);//获取3个版本数据

        Result result = table.get(get);

        //多个列
        Cell[] cells = result.rawCells();

        for (Cell cell : cells) {

            System.out.println("ROWKEY:" + Bytes.toString(CellUtil.cloneRow(cell))
                    + ",CF:" + Bytes.toString(CellUtil.cloneFamily(cell))
                    + ",CN:" + Bytes.toString(CellUtil.cloneQualifier(cell))
                    + ",VALUE:" + Bytes.toString(CellUtil.cloneValue(cell))
            );
        }

    }

    private void getData(String tableName, String rowkey) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));

        Get get = new Get(Bytes.toBytes(rowkey));

        Result result = table.get(get);

        //多个列
        Cell[] cells = result.rawCells();

        for (Cell cell : cells) {

            System.out.println("ROWKEY:" + Bytes.toString(CellUtil.cloneRow(cell))
                    + ",CF:" + Bytes.toString(CellUtil.cloneFamily(cell))
                    + ",CN:" + Bytes.toString(CellUtil.cloneQualifier(cell))
                    + ",VALUE:" + Bytes.toString(CellUtil.cloneValue(cell))
            );
        }

    }

    /**
     * 全表扫描
     *
     * @throws IOException
     */
    @Test
    public void testScanData() throws IOException {
        String tableName = "user_info";
        scanData(tableName);
    }

    private void scanData(String tableName) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));

        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);

        for (Result result : scanner) {

            //多个列
            Cell[] cells = result.rawCells();

            for (Cell cell : cells) {

                System.out.println("ROWKEY:" + Bytes.toString(CellUtil.cloneRow(cell))
                        + ",CF:" + Bytes.toString(CellUtil.cloneFamily(cell))
                        + ",CN:" + Bytes.toString(CellUtil.cloneQualifier(cell))
                        + ",VALUE:" + Bytes.toString(CellUtil.cloneValue(cell))
                );
            }
        }
    }

    /**
     * 删除一条列数据(delete,deleteall合并api)
     */
    @Test
    public void testDeleteData() throws IOException {
        String tableName = "user_info";
        String rowkey = "000001";
        String cf = "f1";
        String cn = "username";

        deleteData(tableName, rowkey, cf, cn);
    }

    private void deleteData(String tableName, String rowkey, String cf, String cn) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));

        Delete delete = new Delete(Bytes.toBytes(rowkey));//删除所有行数据（包括版本）
        //指定列族：列
        delete.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn));//只删除指定列的最新版本
        //delete.addFamily(Bytes.toBytes(cf));
        table.delete(delete);
    }

    /**
     * 删除多行数据
     */
    @Test
    public void testDeleteDatas() throws IOException {
        String tableName = "user_info";
        String rowkey1 = "001";
        String rowkey2 = "002";

        deleteDatas(tableName, rowkey1, rowkey2);
    }

    private void deleteDatas(String tableName, String... rowkeys) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));

        ArrayList<Delete> deletes = new ArrayList<>();
        for (String rowkey : rowkeys) {
            Delete delete = new Delete(Bytes.toBytes(rowkey));
            deletes.add(delete);
        }

        table.delete(deletes);
    }

}
