package org.example.tp_hbase1;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class App {
    public static final String TABLE_NAME="users";
    public static final String CF_PERSONAL_DATA="personal_data";
    public static final String CF_PROFESSIONAL_DATA="professional_data";
    public static void main(String[] args) {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","zookeeper");
        conf.set("hbasezookeeper.property.clientPort","2181");
        conf.set("hbase.master","hbase-master:16000");

        try{
            Connection connection= ConnectionFactory.createConnection(conf);
            Admin admin = connection.getAdmin();
            TableName tableName = TableName.valueOf(TABLE_NAME);
            TableDescriptorBuilder builder=TableDescriptorBuilder.newBuilder(tableName);
            //definir les familles de colonnes
            builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF_PERSONAL_DATA));
            builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF_PROFESSIONAL_DATA));
            TableDescriptor tableDescreptor=builder.build();
            if(!admin.tableExists(tableName)){
                admin.createTable(tableDescreptor);
                System.out.println("table crée");
            }
            else{
                System.out.println("la table est déja crée");
            }

            //inserer les données
            Table table = connection.getTable(tableName);
            Put put = new Put(Bytes.toBytes("11111"));
            put.addColumn(Bytes.toBytes(CF_PERSONAL_DATA),Bytes.toBytes("name"),Bytes.toBytes("ilham oulakbir"));
            put.addColumn(Bytes.toBytes(CF_PERSONAL_DATA),Bytes.toBytes("age"),Bytes.toBytes(21));
            put.addColumn(Bytes.toBytes(CF_PROFESSIONAL_DATA),Bytes.toBytes("diplome"),Bytes.toBytes("ingénieure Data"));
            table.put(put);
            System.out.println("la ligne est insérée avec succès");
            Get get=new Get(Bytes.toBytes("11111"));
            Result result =table.get(get);
            byte[] name=result.getValue(Bytes.toBytes(CF_PERSONAL_DATA),Bytes.toBytes("name"));
            System.out.println(Bytes.toString(name));
        }catch(Exception e){
            e.printStackTrace();
        }
    }
}