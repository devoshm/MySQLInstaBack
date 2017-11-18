package connectors;

import back.IBConstants;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import back.BinlogParser;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class BinlogConnect
{
    public static void initiateReplicationTapping(Map<String, Object> credentials) throws IOException
    {
        System.out.println("Tapping into MySQL Replication!");
        BinaryLogClient client = new BinaryLogClient((String) credentials.get(IBConstants.HOST), (Integer) credentials.get(IBConstants.PORT), (String) credentials.get(IBConstants.USERNAME), (String) credentials.get(IBConstants.PASSWORD));
        client.registerEventListener(BinlogParser::parseEvent);
        if (credentials.containsKey(IBConstants.DB_NAMES))
        {
            BinlogParser.schemaNames = (List<String>) credentials.get(IBConstants.DB_NAMES);
        }
        client.connect();
    }
}
