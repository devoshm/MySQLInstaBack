package connectors;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

public class CassandraConnect
{
    private Cluster cluster;
    private Session session;

    public void connect(String node, Integer port) {
        Cluster.Builder b = Cluster.builder().addContactPoint(node);
        if (port != null) {
            b.withPort(port);
        }
        cluster = b.build();

        session = cluster.connect();
    }

    public Cluster getCluster()
    {
        return cluster;
    }

    public Session getSession()
    {
        return this.session;
    }

    public void setSession(Session session)
    {
        this.session = session;
    }

    public ResultSet execute(String query)
    {
        return session.execute(query);
    }

    public void close() {
        session.close();
        cluster.close();
    }
}
