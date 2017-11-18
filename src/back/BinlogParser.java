package back;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.QueryEventData;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BinlogParser
{
    public static List<String> schemaNames = null;

    public static void parseEvent(Event event)
    {
        Map<String, Object> queryData;
        EventHeaderV4 eventHeader;
        Object header = event.getHeader();

        if (header instanceof EventHeaderV4)
        {
            eventHeader = ((EventHeaderV4) header);
            EventType eventType = eventHeader.getEventType();
            System.out.println(eventType);

            if (eventType == EventType.QUERY)
            {
                QueryEventData queryEventData = event.getData();
                String sql = queryEventData.getSql();
                if (sql.startsWith("BEGIN"))
                {
                    return;
                }

                String schemaName = queryEventData.getDatabase();
                if(schemaNames != null && !schemaNames.contains(schemaName))
                {
                    return;
                }

                queryData = new HashMap<>();
                queryData.put(IBConstants.TIMESTAMP, eventHeader.getTimestamp());
                queryData.put(IBConstants.TIME_TAKEN, queryEventData.getExecutionTime());
                queryData.put(IBConstants.DATABASE, schemaName);
                queryData.put(IBConstants.QUERY, sql);
                BinlogQueue.addToQueue(queryData);
            }
        }
    }
}
