package config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class DatabaseConfig {

    private Table table1;
    private String tableName = "employee";
    private String family1 = "PersonalData";
    private String family2 = "ProfessionalData";

    public void addData(String name, String salary) throws IOException {
        Configuration config = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(config);
        Admin admin = connection.getAdmin();

        HTableDescriptor ht = new HTableDescriptor(TableName.valueOf(tableName));
        ht.addFamily(new HColumnDescriptor(family1));
        ht.addFamily(new HColumnDescriptor(family2));
        System.out.println("connecting");

        table1 = connection.getTable(TableName.valueOf(tableName));

        try {
            byte[] row1 = Bytes.toBytes(name);
            Put p = new Put(row1);

            p.addColumn(family1.getBytes(), "name".getBytes(), Bytes.toBytes(name));
            p.addColumn(family2.getBytes(), "salary".getBytes(), Bytes.toBytes(salary));
            table1.put(p);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            table1.close();
            connection.close();
        }
    }

    public static void main(String name, String salary) throws IOException {
        DatabaseConfig admin = new DatabaseConfig();
        admin.addData(name, salary);
    }
}
