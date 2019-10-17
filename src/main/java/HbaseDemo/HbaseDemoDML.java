package HbaseDemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author HaoxuanLi  Github:bestksl
 * @version created date：2019-10-16 15:24
 */
public class HbaseDemoDML {
    private Connection conn = null;

    @Before
    public void getConnection() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "hadoop-01:2181,hadoop-02:2181,hadoop-03:2181");
        conn = ConnectionFactory.createConnection(conf);
    }

    /*
    增
     */
    @Test
    public void testPut() throws IOException {
        // 获取table对象
        Table table = conn.getTable(TableName.valueOf("user_info1"));

        //创建put对象 同时指定 row key  一个put对象只能对应一个row key
        Put put1 = new Put(Bytes.toBytes("002"));
        Put put2 = new Put(Bytes.toBytes("003"));
        Put put3 = new Put(Bytes.toBytes("004"));

        // 分别为 列族名, 属性key, 属性值
        put1.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("name"), Bytes.toBytes("phll"));
        put1.addColumn(Bytes.toBytes("extra_info"), Bytes.toBytes("addr"), Bytes.toBytes("qz"));
        put1.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("age"), Bytes.toBytes("24"));
        put2.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("name"), Bytes.toBytes("wlxx"));
        put2.addColumn(Bytes.toBytes("extra_info"), Bytes.toBytes("addr"), Bytes.toBytes("ltfam"));
        put2.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("age"), Bytes.toBytes("142"));
        put3.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("name"), Bytes.toBytes("lt"));
        put3.addColumn(Bytes.toBytes("extra_info"), Bytes.toBytes("addr"), Bytes.toBytes("lf"));
        put3.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("age"), Bytes.toBytes("142"));

        // 想表中插入put对象 注意可以单独插入一个put, 也可以插入一个List[put1,put2]
        table.put(put1);
        List<Put> puts = new ArrayList<Put>();
        puts.add(put2);
        puts.add(put3);
        table.put(puts);

        // 关闭连接
        table.close();
        conn.close();
    }

    /*
    删
     */
    @Test
    public void testDelete() throws IOException {

        // 获取 table 对象
        Table table = conn.getTable(TableName.valueOf("user_info1"));

        // 生成delete对象
        Delete delete1 = new Delete(Bytes.toBytes("002")); // 如果不指定细节, 例如列族, 将会删掉row key对应的所有数据
        delete1.addColumn(Bytes.toBytes("extra_info"), Bytes.toBytes("addr"));
        Delete delete2 = new Delete(Bytes.toBytes("003"));

        // 同样也可以单传 或者 list传
        List<Delete> deletes = new ArrayList<Delete>();
        deletes.add(delete1);
        deletes.add(delete2);

        // 提交
        table.delete(deletes);

        // 关闭连接
        table.close();
        conn.close();

    }


    /*
    改
     */
    @Test
    public void testUpdate() {


    }


    /*
    查单个row
     */
    @Test
    public void testGet() throws IOException {

        // 创建需要获取数据的表对象
        Table table = conn.getTable(TableName.valueOf("user_info2"));

        // 生成get对象  一个get 对象只能对应一个 row key
        Get get = new Get("003".getBytes());

        // 提交 并返回一个 result 对象
        Result result = table.get(get);

        // 获取指定value
        String addr = new String(result.getValue("extra_info".getBytes(), "addr".getBytes()));
        System.out.println(addr);

        // 从 result 对象获取 CellScanner 迭代器
        CellScanner cs = result.cellScanner();

        // 遍历cell对象
        while (cs.advance()) {
            Cell cell = cs.current();
            System.out.println("-----------------------------------");
            System.out.println(new String(cell.getRowArray()));   // row key
            System.out.println(new String(cell.getFamilyArray())); // 列族名
            System.out.println(new String(cell.getQualifierArray())); // 属性 key
            System.out.println(new String(cell.getValueArray())); // 属性 value
        }

        // 关闭连接
        table.close();
        conn.close();
    }

    /*
    范围查询
     */
    @Test
    public void testScan() throws IOException {

        // 生成表对象
        Table table = conn.getTable(TableName.valueOf("user_info2"));

        // 生成范围查找需要的 范围对象 Scan (包含起始, 不包含终点需要增加一个 \s000 )
        Scan scan = new Scan("001".getBytes(), "005\000".getBytes());

        // 提交 并接收一个 ResultScanner 对象 (也就是 多个 row 的 result 集合)
        ResultScanner rs = table.getScanner(scan);

        // 遍历 ResultScanner 中的 result对象
        for (Result result : rs) {

            // 通过单个result对象 获得 CellScanner 迭代器
            CellScanner cs = result.cellScanner();
            while (cs.advance()) {
                Cell cell = cs.current();
                System.out.println("----------------------------------");
                //这里估计还是老API, 如果不指定 属性偏移量和长度 会乱码
                System.out.println(new String(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()));   // row key
                System.out.println(new String(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength())); // 列族名
                System.out.println(new String(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())); // 属性 key
                System.out.println(new String(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength())); // 属性 value
            }
        }

        // 关闭资源
        table.close();
        conn.close();
    }
}
