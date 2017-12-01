package connectors;

import back.IBConstants;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.*;

public class MySQLConnect
{
    private String host;
    private int port;
    private String userName;
    private String password;
    private List<String> schemaNames;

    public MySQLConnect(Map<String, Object> connectionDetails)
    {
        this.host = (String) connectionDetails.get(IBConstants.HOST);
        this.port = (int) connectionDetails.get(IBConstants.PORT);
        this.userName = (String) connectionDetails.get(IBConstants.USERNAME);
        this.password = (String) connectionDetails.get(IBConstants.PASSWORD);
        this.schemaNames = (List<String>) connectionDetails.getOrDefault(IBConstants.DB_NAMES, new ArrayList<>());
    }

    public void initiateRestore(long timeStamp, String... dbNames) throws Exception
    {
        String cmd = "mysql";
        File file = new File("sql_dumps/backup_config.txt");
        FileReader fileReader = new FileReader(file);
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        String line = bufferedReader.readLine();
        fileReader.close();

        if (line == null)
        {
            throw new FileNotFoundException("No backups are found!");
        }

        long backupTime = Long.parseLong(line.substring(line.indexOf("_") + 1, line.indexOf(".")));
        if (timeStamp < backupTime)
        {
            throw new IllegalArgumentException("No backup found for the given time. Unable to restore!");
        }

        String restoreFile = " < sql_dumps\\" + line;
        if (dbNames.length > 0)
        {
            for (String schema : dbNames)
            {
                if (!schemaNames.contains(schema))
                {
                    System.out.println("The schema " + schema + " is not found in backup list. Proceeding to the next.");
                    continue;
                }

                String dumpQuery = "--one-database " + schema + restoreFile;
                backupOrRestore(cmd, dumpQuery);
            }
        }
        else
        {
            backupOrRestore(cmd, restoreFile);
        }

        System.out.println("Snapshot Restored!");
    }

    private Map<Long, String> getQueriesInOrder(ResultSet restoreQueries)
    {
        Map<Long, String> queryMap = new TreeMap<>();

        long size = 0L;
        for (Row row : restoreQueries)
        {
            size++;
            System.out.println(row);
            String dbNameWithQuery = row.getString("SchemaName") + IBConstants.RESTORE_DELIMITER + row.getString("Query");
            queryMap.put(row.getLong("Timestamp"), dbNameWithQuery);
        }

        if (size != queryMap.size())
        {
            throw new IllegalStateException("Difference in restored and sorted queries size!");
        }

        return queryMap;
    }

    public void runRestoreQueries(ResultSet restoreQueries) throws Exception
    {
        Map<Long, String> orderedQueries = getQueriesInOrder(restoreQueries);

        Connection con = DriverManager.getConnection("jdbc:mysql://" + host + ":" + port, userName, password);
        Statement stmt = con.createStatement();
        String currentSchema = null;

        System.out.println("Running Restore Queries..!");
        long prevTimeStamp = -1L, currTimeStamp;
        for (Map.Entry<Long, String> mapEntry : orderedQueries.entrySet())
        {
            System.out.println(mapEntry);
            currTimeStamp = mapEntry.getKey();
            if (currTimeStamp < prevTimeStamp)
            {
                throw new IllegalStateException("Order mismatch!");
            }
            prevTimeStamp = currTimeStamp;

            String schema = mapEntry.getValue().split(IBConstants.RESTORE_DELIMITER)[0];
            if (!schema.equalsIgnoreCase(currentSchema))
            {
                currentSchema = schema;
                con.setCatalog(currentSchema);
                stmt = con.createStatement();
            }
            stmt.executeUpdate(mapEntry.getValue().split(IBConstants.RESTORE_DELIMITER)[1]);
        }
        con.close();
        System.out.println("Restore Completed!");
    }

    public void makeInitialBackup() throws Exception
    {
        StringBuilder dumpQuery = new StringBuilder();
        if (schemaNames.isEmpty())
        {
            dumpQuery.append("--all-databases");
        }
        else
        {
            dumpQuery.append("--databases");
            for (String schema : schemaNames)
            {
                dumpQuery.append(" ");
                dumpQuery.append(schema);
            }
        }

        long currentTime = System.currentTimeMillis();
        String fileName = "backup_" + currentTime + ".sql";
        dumpQuery.append(" > sql_dumps\\");
        dumpQuery.append(fileName);

        backupOrRestore("mysqldump", dumpQuery.toString());

        System.out.println("Snapshot created in " + fileName);
        FileWriter f2 = new FileWriter(new File("sql_dumps/backup_config.txt"), false);
        f2.write(fileName);
        f2.close();
    }

    private void backupOrRestore(String command, String placeholder) throws Exception
    {
        String dumpQuery = "cmd.exe /c \"C:\\Program Files\\MariaDB 10.2\\bin\\%s\" -u" + userName +
                " -p" +
                password +
                " %s";

        String sqlCommand = String.format(dumpQuery, command, placeholder);
        System.out.println(sqlCommand);
        Process runtimeProcess = Runtime.getRuntime().exec(sqlCommand);
        int processStatus = runtimeProcess.waitFor();

        if (processStatus != 0)
        {
            BufferedReader reader =
                    new BufferedReader(new InputStreamReader(runtimeProcess.getErrorStream()));

            String line;
            while ((line = reader.readLine()) != null)
            {
                System.out.println(line);
            }

            throw new IOException("Unable to create/restore a snapshot!");
        }
    }
}
