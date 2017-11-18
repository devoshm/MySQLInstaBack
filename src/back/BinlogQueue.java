package back;

import java.util.Map;
import java.util.Observable;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class BinlogQueue extends Observable
{
    private static BlockingQueue<Map<String, Object>> blockingQueue = new LinkedBlockingQueue();

    public static void addToQueue(Map<String, Object> queryData)
    {
        blockingQueue.add(queryData);
    }

    public static Map<String, Object> getFromQueue() throws InterruptedException
    {
        return blockingQueue.take();
    }
}
