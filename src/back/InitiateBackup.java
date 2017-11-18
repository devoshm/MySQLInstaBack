package back;

import connectors.BinlogConnect;
import connectors.MySQLConnect;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class InitiateBackup
{
    private static Map<String, Map<String, Object>> connectionDetails = new HashMap<>();
    private static Scanner scanner = new Scanner(System.in);

    public static void main(String[] args)
    {
        try
        {
            readConfigs();
            System.out.println("Welcome to Insta Backup!\nPlease select an option below:\n1. Backup\n2. Restore\nEnter your choice: ");
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
        MySQLConnect.makeInitialBackup(connectionDetails.get(IBConstants.MY_SQL));

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
            PushToCassandra.initializeCassandra(connectionDetails.get(IBConstants.CASSANDRA));
            PushToCassandra.getDataFromQueue();
        });
        threadCassandra.start();
    }

    private static void initiateRestore() throws Exception
    {
        System.out.println("Enter timestamp up to which you want to initiate restore: ");
        long timestamp = scanner.nextLong();
        MySQLConnect.initiateRestore(connectionDetails.get(IBConstants.MY_SQL), timestamp);

    }

    private static void readConfigs() throws IOException
    {
        try(FileInputStream fis = new FileInputStream("resources/ConnectionCredentials.yml"))
        {
            connectionDetails = new Yaml().load(fis);
        }
    }
}
