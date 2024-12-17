package org.apache.zookeeper.client;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class ZookeeperClientServerIT {
    private String port;

    public ZookeeperClientServerIT(String port) {
        this.port = port;
    }

    @Parameterized.Parameters
    public static Collection configure() {
        return Arrays.asList(new Object[][]{
                {"12352"}
        });
    }

    @Test
    public void testConnectionClientServer() throws IOException, InterruptedException, KeeperException {
        int tickTime = 1000;
        int numConnections = 2000;

        //configuro la directory del server
        File dir = new File(System.getProperty("java.io.tmpdir"), "zookeeperIntegr").getAbsoluteFile();
        ZooKeeperServer server = new ZooKeeperServer(dir, dir, tickTime);

        //mock del serverCnxnFactory
        ServerCnxnFactory standaloneServerFactory = Mockito.spy(ServerCnxnFactory.createFactory(Integer.parseInt(this.port), numConnections));
        standaloneServerFactory.startup(server);

        //avvio il client
        ZooKeeper client = new ZooKeeper("127.0.0.1:" + this.port, 5000, event -> {
        });

        //recupero le connessioni attive e verifico che il metodo registerConnection sia stato chiamato
        Iterable<ServerCnxn> connections = standaloneServerFactory.getConnections();
        int connectionCount = 0;
        for (ServerCnxn conn : connections) {
            connectionCount++;
        }
        System.out.println("Stato del server: " + (server.isRunning() ? "Running" : "Not Running"));

        System.out.println("Stato client: " + client.getState());

        // Stampo il numero di connessioni attive
        System.out.println("Numero di connessioni attive: " + connectionCount);
        for (ServerCnxn conn : connections) {
            Mockito.verify(standaloneServerFactory, Mockito.times(1)).registerConnection(conn);
        }

        //chiudo il client e il server
        client.close();
        standaloneServerFactory.shutdown();
    }



    @Test
    public void testNumberConnectionClientServer() throws IOException, InterruptedException, KeeperException {
        int tickTime = 1000;
        int numConnections = 2000;

        //creo un'istanza del server
        File dir = new File(System.getProperty("java.io.tmpdir"), "zookeeperIntegr").getAbsoluteFile();
        ZooKeeperServer server = new ZooKeeperServer(dir, dir, tickTime);

        //creo un'istanza del serverCnxnFactory
        ServerCnxnFactory standaloneServerFactory = ServerCnxnFactory.createFactory(Integer.parseInt(this.port) + 1, numConnections);

        //avvio il server
        standaloneServerFactory.startup(server);

        //conto il nr di connessioni. inizialmente 0
        int numConnection = standaloneServerFactory.getNumAliveConnections();

        //avvio un client
        int port = Integer.parseInt(this.port) + 1;
        ZooKeeper client = new ZooKeeper("127.0.0.1:" + port, 5000, event -> {

        });
        //mi assicuro che la connessione venga stabilita e registrata sul server
        client.exists("/", false);

        //recuper nuovamente il numero di connessioni attive
        int numConnFinal = standaloneServerFactory.getNumAliveConnections();
        client.close();
        standaloneServerFactory.shutdown();
        Assert.assertEquals(numConnection + 1, numConnFinal);
    }
}
