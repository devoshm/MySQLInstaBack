package connectors;

import back.IBConstants;

import java.io.*;
import java.util.List;
import java.util.Map;

public class MySQLConnect
{
    public static void makeInitialBackup(Map<String, Object> connectionDetails) throws Exception
    {
        StringBuilder dumpQuery = new StringBuilder();
        if (connectionDetails.containsKey(IBConstants.DB_NAMES))
        {
            dumpQuery.append("--databases");
            List<String> schemas = (List<String>) connectionDetails.get(IBConstants.DB_NAMES);
            for (String schema : schemas)
            {
                dumpQuery.append(" ");
                dumpQuery.append(schema);
            }
        }
        else
        {
            dumpQuery.append("--all-databases");
        }

        long currentTime = System.currentTimeMillis();
        String fileName = "backup_" + currentTime + ".sql";
        dumpQuery.append(" > sql_dumps\\");
        dumpQuery.append(fileName);

        backupOrRestore(connectionDetails, "mysqldump", dumpQuery.toString());

        System.out.println("Snapshot created in " + fileName);
        FileWriter f2 = new FileWriter(new File("sql_dumps/backup_config.txt"), false);
        f2.write(fileName);
        f2.close();
    }

    public static void initiateRestore(Map<String, Object> connectionDetails, long timeStamp) throws Exception
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
        if (connectionDetails.containsKey(IBConstants.DB_NAMES))
        {
            List<String> schemas = (List<String>) connectionDetails.get(IBConstants.DB_NAMES);
            for (String schema : schemas)
            {
                String dumpQuery = "--one-database " + schema + restoreFile;
                backupOrRestore(connectionDetails, cmd, dumpQuery);
            }
        }
        else
        {
            backupOrRestore(connectionDetails, cmd, restoreFile);
        }

        System.out.println("Snapshot Restored!");
    }

    private static void backupOrRestore(Map<String, Object> connectionDetails, String command, String placeholder) throws Exception
    {
        String dumpQuery = "cmd.exe /c \"C:\\Program Files\\MariaDB 10.2\\bin\\%s\" -u" + connectionDetails.get(IBConstants.USERNAME) +
                " -p" +
                connectionDetails.get(IBConstants.PASSWORD) +
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
