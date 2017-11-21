package back;

import com.datastax.driver.core.ResultSet;
import connectors.BinlogConnect;
import connectors.MySQLConnect;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class Initiator
{
    private static Map<String, Map<String, Object>> connectionDetails = new HashMap<>();
    private static Scanner scanner = new Scanner(System.in);

    public static void main(String[] args)
    {
        try
        {
            readConfigs();
            System.out.print("Welcome to Insta Backup!\nPlease select an option below:\n1. Backup\n2. Restore\nEnter your choice: ");
            int opt = scanner.nextInt();
            switch (opt)
            {
                case 1:
                    initiateBackup();
                    break;
                case 2:
                    initiateRestore();
                    break;
                default:
                    throw new IllegalArgumentException("Invalid Choice!");
            }
        } catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    private static void initiateBackup() throws Exception
    {
        File file = new File("sql_dumps/");
        if (!file.exists())
        {
            if (file.mkdir())
            {
                System.out.println("Directory for SQL Dump created!");
            }
            else
            {
                throw new IOException("Unable to create directory!");
            }
        }
        MySQLConnect mySQLConnect = new MySQLConnect(connectionDetails.get(IBConstants.MY_SQL));
        mySQLConnect.makeInitialBackup();

        Thread threadBinlog = new Thread(() -> {
            try
            {
                BinlogConnect.initiateReplicationTapping(connectionDetails.get(IBConstants.MY_SQL));
            } catch (IOException e)
            {
                e.printStackTrace();
            }
        });
        threadBinlog.start();

        Thread threadCassandra = new Thread(() -> {
            CassandraPushOrPop cassandraPushOrPop = null;
            try
            {
                cassandraPushOrPop = new CassandraPushOrPop(connectionDetails.get(IBConstants.CASSANDRA));
                cassandraPushOrPop.initializeCassandra(connectionDetails.get(IBConstants.CASSANDRA));
                cassandraPushOrPop.pushDataToCluster();
            }
            finally
            {
                if (cassandraPushOrPop != null)
                {
                    cassandraPushOrPop.closeConnection();
                }
            }
        });
        threadCassandra.start();
    }

    private static void initiateRestore() throws Exception
    {
        System.out.print("Enter timestamp up to which you want to initiate restore: ");
        long timestamp = scanner.nextLong();
        MySQLConnect mySQLConnect = new MySQLConnect(connectionDetails.get(IBConstants.MY_SQL));
        CassandraPushOrPop cassandraPushOrPop = new CassandraPushOrPop(connectionDetails.get(IBConstants.CASSANDRA));
        System.out.print("Do you want to restore all the databases(1) or only a few (2)?");

        switch (scanner.nextInt())
        {
            case 1:
                mySQLConnect.initiateRestore(timestamp);
                ResultSet resultSet = cassandraPushOrPop.getQueriesTillRestorePoint(timestamp);
                mySQLConnect.runRestoreQueries(resultSet);
                break;
            case 2:
                System.out.print("Enter the database names separated by comma: ");
                String[] dbNames = scanner.next().split(",");
                mySQLConnect.initiateRestore(timestamp, dbNames);
                cassandraPushOrPop.getQueriesTillRestorePoint(timestamp, dbNames);
                break;
        }
        cassandraPushOrPop.closeConnection();
    }

    private static void readConfigs() throws IOException
    {
        try(FileInputStream fis = new FileInputStream("resources/ConnectionCredentials.yml"))
        {
            connectionDetails = new Yaml().load(fis);
        }
    }
}
