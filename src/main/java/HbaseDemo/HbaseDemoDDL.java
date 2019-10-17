package HbaseDemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * @author HaoxuanLi  Github:bestksl
 * @version created date：2019-10-15 15:59
 */
public class HbaseDemoDDL {
    private Connection conn = null;

    @Before
    public void getConn() throws IOException {
        Configuration conf = HBaseConfiguration.create();  //会自动加载 hbase-site.xml
        conf.set("hbase.zookeeper.quorum", "hadoop-01:2181,hadoop-02:2181,hadoop-03:2181");
        conn = ConnectionFactory.createConnection(conf);

    }

    @Test
    public void testCreateTable() throws IOException {

        // 创建一个DDL操作器
        Admin admin = conn.getAdmin();

        // 创建Table对象
        TableName tname = TableName.valueOf("user_info1");
        TableDescriptorBuilder tableDescBuilder = TableDescriptorBuilder.newBuilder(tname);

        // 创建列族描述对象
        // 列族1
        ColumnFamilyDescriptorBuilder columnDescBuilder1 = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("base_info"));
        columnDescBuilder1.setMaxVersions(3);
        // 列族2
        ColumnFamilyDescriptorBuilder columnDescBuilder2 = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("extra_info"));
        columnDescBuilder1.setMaxVersions(3);


        // 将列族加入table
        tableDescBuilder.setColumnFamily(columnDescBuilder1.build()).setColumnFamily(columnDescBuilder2.build());

        // 生成table describer 对象
        TableDescriptor tableDescriptor = tableDescBuilder.build();

        // 创建表信息描述对象
        admin.createTable(tableDescriptor);
        admin.close();
        conn.close();
    }

    /**
     * DDL  测试
     */

    @Test
    public void testDropTable() throws IOException {
        Admin admin = conn.getAdmin();
        // 安全机制需要先停用再删除
        admin.disableTable(TableName.valueOf("user_infos"));
        admin.deleteTable(TableName.valueOf("user_infos"));

    }


    @Test
    public void testUpdateTable() throws IOException {
        Admin admin = conn.getAdmin();

        // 获取需要修改表的描述对象
        TableDescriptor td = admin.getDescriptor(TableName.valueOf("user_info2"));

        // 以旧的描述对象为基础创建新的描述对象
        TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(td);
        ColumnFamilyDescriptorBuilder columnDescBuilder1 = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("third_info"));
        tdb.setColumnFamily(columnDescBuilder1.build());
        TableDescriptor newDesc = tdb.build();
        admin.modifyTable(newDesc);


    }

}
