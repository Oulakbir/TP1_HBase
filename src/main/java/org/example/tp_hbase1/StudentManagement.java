package org.example.tp_hbase1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.CellUtil;

import java.io.IOException;


public class StudentManagement {
    public static final String TABLE_NAME="students";
    public static final String CF_INFO="info";
    public static final String CF_GRADES="grades";

    public static void main(String[] args) {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","zookeeper");
        conf.set("hbase.zookeeper.property.clientPort","2181");
        conf.set("hbase.master","hbase-master:16000");

        try{
            Connection connection = ConnectionFactory.createConnection(conf);
            Admin admin = connection.getAdmin();
            TableName tableName = TableName.valueOf(TABLE_NAME);
            TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
            // Définir les familles de colonnes
            builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF_INFO));
            builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF_GRADES));
            TableDescriptor tableDescriptor = builder.build();

            if(!admin.tableExists(tableName)){
                admin.createTable(tableDescriptor);
                System.out.println("Table créée");
            } else {
                System.out.println("La table est déjà créée");
            }

            // Ajouter les étudiants
            Table table = connection.getTable(tableName);

            // Étudiant 1
            Put put1 = new Put(Bytes.toBytes("student1"));
            put1.addColumn(Bytes.toBytes(CF_INFO), Bytes.toBytes("name"), Bytes.toBytes("John Doe"));
            put1.addColumn(Bytes.toBytes(CF_INFO), Bytes.toBytes("age"), Bytes.toBytes("20"));
            put1.addColumn(Bytes.toBytes(CF_GRADES), Bytes.toBytes("math"), Bytes.toBytes("B"));
            put1.addColumn(Bytes.toBytes(CF_GRADES), Bytes.toBytes("science"), Bytes.toBytes("A"));
            table.put(put1);

            // Étudiant 2
            Put put2 = new Put(Bytes.toBytes("student2"));
            put2.addColumn(Bytes.toBytes(CF_INFO), Bytes.toBytes("name"), Bytes.toBytes("Jane Smith"));
            put2.addColumn(Bytes.toBytes(CF_INFO), Bytes.toBytes("age"), Bytes.toBytes("22"));
            put2.addColumn(Bytes.toBytes(CF_GRADES), Bytes.toBytes("math"), Bytes.toBytes("A"));
            put2.addColumn(Bytes.toBytes(CF_GRADES), Bytes.toBytes("science"), Bytes.toBytes("A"));
            table.put(put2);

            // Récupérer et afficher les informations pour student1
            Get getStudent1 = new Get(Bytes.toBytes("student1"));
            Result resultStudent1 = table.get(getStudent1);
            System.out.println("Informations pour student1:");
            for (Cell cell : resultStudent1.listCells()) {
                System.out.println("Column Family: " + Bytes.toString(CellUtil.cloneFamily(cell)) +
                        ", Column: " + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                        ", Value: " + Bytes.toString(CellUtil.cloneValue(cell)));
            }

            // Mettre à jour l'âge de Jane Smith et sa note de math
            Put updateJane = new Put(Bytes.toBytes("student2"));
            updateJane.addColumn(Bytes.toBytes(CF_INFO), Bytes.toBytes("age"), Bytes.toBytes("23"));
            updateJane.addColumn(Bytes.toBytes(CF_GRADES), Bytes.toBytes("math"), Bytes.toBytes("A+"));
            table.put(updateJane);
            System.out.println("Jane Smith mise à jour avec succès.");

            // Supprimer l'étudiant avec la Row Key "student1"
            Delete deleteStudent1 = new Delete(Bytes.toBytes("student1"));
            table.delete(deleteStudent1);
            System.out.println("Student1 supprimé avec succès.");

            // Afficher toutes les informations pour tous les étudiants
            Scan scan = new Scan();
            ResultScanner scanner = table.getScanner(scan);
            System.out.println("Informations pour tous les étudiants:");
            for (Result result : scanner) {
                for (Cell cell : result.listCells()) {
                    System.out.println("Row Key: " + Bytes.toString(result.getRow()) +
                            ", Column Family: " + Bytes.toString(CellUtil.cloneFamily(cell)) +
                            ", Column: " + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                            ", Value: " + Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
            scanner.close();

        } catch(Exception e){
            e.printStackTrace();
        }
    }
}


