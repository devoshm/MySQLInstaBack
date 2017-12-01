package back;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import connectors.CassandraConnect;

import java.util.Map;

class CassandraPushOrPop
{
    private static String CASSANDRA_INSERT = "INSERT INTO CommandHistory (UID, Timestamp, ExecutionTime, SchemaName, Query) VALUES (NOW(), %s, %s, '%s', $$%s$$);";
    private static String CASSANDRA_GET = "SELECT * FROM CommandHistory WHERE Timestamp <= %s %sallow filtering;";

    private CassandraConnect cassandraConnect = new CassandraConnect();
    private String keySpace;

    CassandraPushOrPop(Map<String, Object> connectionDetails)
    {
        cassandraConnect.connect((String) connectionDetails.get(IBConstants.HOST), (Integer) connectionDetails.getOrDefault(IBConstants.PORT, null));
        this.keySpace = (String) connectionDetails.get(IBConstants.KEYSPACE);
    }

    private static String getCassandraInsert(Map<String, Object> queryData)
    {
        //Long timeStamp = (Long) queryData.get(IBConstants.TIMESTAMP);
        Long timeTaken = (Long) queryData.get(IBConstants.TIME_TAKEN);
        String dbName = (String) queryData.get(IBConstants.DATABASE);
        String query = (String) queryData.get(IBConstants.QUERY);

        String insertQuery = String.format(CASSANDRA_INSERT, System.currentTimeMillis(), timeTaken, dbName, query);
        System.out.println(insertQuery);
        return insertQuery;
    }

    void initializeCassandra(Map<String, Object> connectionDetails)
    {
        try
        {
            cassandraConnect.setSession(cassandraConnect.getCluster().connect(keySpace));
        }
        catch (InvalidQueryException ex)
        {
            if (ex.getMessage().equals("Keyspace '" + keySpace.toLowerCase() + "' does not exist"))
            {
                StringBuilder query = new StringBuilder("CREATE KEYSPACE ");
                query.append(keySpace);
                if (connectionDetails.containsKey(IBConstants.REPL_STRATEGY))
                {
                    query.append("  WITH replication = {'class':'");
                    query.append(connectionDetails.get(IBConstants.REPL_STRATEGY));
                    query.append("', 'replication_factor' : ");
                    query.append(connectionDetails.get(IBConstants.REPL_FACTOR));
                    query.append("};");
                }
                cassandraConnect.execute(query.toString());
                cassandraConnect.setSession(cassandraConnect.getCluster().connect(keySpace));
                cassandraConnect.execute("CREATE TABLE CommandHistory (UID UUID PRIMARY KEY, Timestamp BIGINT, ExecutionTime BIGINT, SchemaName VARCHAR, Query VARCHAR);");
                System.out.println("Keyspace and Table created successfully!");
            }
            else
            {
                throw ex;
            }
        }
    }

    void pushDataToCluster()
    {
        Map<String, Object> queryData;
        while (true)
        {
            try
            {
                queryData = BinlogQueue.getFromQueue();
                cassandraConnect.execute(getCassandraInsert(queryData));
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
            }
        }
    }

    ResultSet getQueriesTillRestorePoint(long timeStamp, String... dbNames)
    {
        cassandraConnect.setSession(cassandraConnect.getCluster().connect(keySpace));

        StringBuilder stringBuilder = new StringBuilder();
        if (dbNames.length > 0)
        {
            stringBuilder.append("and (");
            for (String schema : dbNames)
            {
                stringBuilder.append("SchemaName = '");
                stringBuilder.append(schema);
                stringBuilder.append("' or ");
            }
            int lastIndexOfOr = stringBuilder.lastIndexOf(" or ");
            stringBuilder.replace(lastIndexOfOr, lastIndexOfOr + 5, ")");
        }

        String query = String.format(CASSANDRA_GET, timeStamp, stringBuilder);
        System.out.println("Restore Point Query From Cassandra: " + query);
        return cassandraConnect.execute(query);
    }

    void closeConnection()
    {
        cassandraConnect.close();
    }
}
