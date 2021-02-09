package com.zq;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HBaseApp {

    private final static Logger log = LoggerFactory.getLogger(HBaseApp.class);

    private Connection connection;
    private Table table;
    private Admin admin;
    String tableName = "user";

    @Before()
    public void doBefore(){
        Configuration configuration = new Configuration();
        configuration.set("hbase.rootdir","hdfs://hadoop000:9000/hbase");
        configuration.set("hbase.zookeeper.quorum","hadoop000:2181");
        System.setProperty("hadoop.home.dir", "C:\\Users\\Administrator\\Downloads\\hadoop-3.3.0");
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();

            Assert.assertNotNull(connection);
            Assert.assertNotNull(admin);
        } catch (IOException e) {
            log.error("连接超时：",e);
        }
    }

    @Test
    public void createTable(){
        TableName table = TableName.valueOf(this.tableName);
        try {
            if (admin.tableExists(table)){
                System.out.println(this.tableName +"表已存在。。。");
            }else {
                HTableDescriptor tableDescriptor = new HTableDescriptor(table);
                tableDescriptor.addFamily(new HColumnDescriptor("info"));
                tableDescriptor.addFamily(new HColumnDescriptor("address"));
                admin.createTable(tableDescriptor);
                System.out.println(this.tableName +"表创建成功。。。");
            }
        } catch (IOException e) {
         log.error("执行失败：",e);
        }
    }

    @Test
    public void queryTableInfo() throws IOException {
        HTableDescriptor[] tables = admin.listTables();
        if (tables.length > 0) {
            for (HTableDescriptor tableDescriptor : tables) {
                System.out.println(tableDescriptor.getNameAsString());
                HColumnDescriptor[] columnFamilies = tableDescriptor.getColumnFamilies();
                for (HColumnDescriptor hColumnDescriptor : columnFamilies) {
                    System.out.println("\t"+hColumnDescriptor.getNameAsString());
                }
            }
        }
    }

    @Test
    public void addTableInfo() throws IOException {
        table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes("zq"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"),Bytes.toBytes("zzqq"));

        table.put(put);
    }

    @Test
    public void testGet(){
        try {
            table = connection.getTable(TableName.valueOf("first_tab"));
            Get get = new Get(Bytes.toBytes("zq"));
            get.addColumn(Bytes.toBytes("member"), Bytes.toBytes("city"));
            Result result = table.get(get);
            printResult(result);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void testScan(){
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            // [start,end)
            Scan scan = new Scan(Bytes.toBytes("start"),Bytes.toBytes("end"));
            Scan scan2 = new Scan(new Get(Bytes.toBytes("zq")));
            Scan scan3 = new Scan(new Get(Bytes.toBytes("zq")));
            Scan scan4 = new Scan();
            scan4.addFamily(Bytes.toBytes("info"));
            scan4.addColumn(Bytes.toBytes("info"),Bytes.toBytes("info"));

            ResultScanner rs = table.getScanner(scan);
            for (Result result : rs) {
                System.out.println(result);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void testFilter() throws IOException, DeserializationException {
        table = connection.getTable(TableName.valueOf(tableName));
        // [start,end)
        Scan scan = new Scan();
        Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, ByteArrayComparable.parseFrom(Bytes.toBytes("zzqq")));

        String reg = "^z";
        Filter filter2 = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(reg));
        // FilterList 是 Filter的子类
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        filterList.addFilter(filter);
        filterList.addFilter(filter2);
        scan.setFilter(filter);
        ResultScanner resultSet = table.getScanner(scan);
        System.out.println(resultSet);
    }

    @After()
    public void doAfter(){
        if (table != null) {
            try {
                table.close();
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

        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    private void printResult(Result result) {
        for (Cell cell : result.rawCells()) {
            System.out.println(Bytes.toString(result.getRow()) + "\t "
                    + Bytes.toString(CellUtil.cloneFamily(cell)) + "\t"
                    + Bytes.toString(CellUtil.cloneQualifier(cell)) + "\t"
                    + Bytes.toString(CellUtil.cloneValue(cell)) + "\t"
                    + cell.getTimestamp());
        }
    }
}
