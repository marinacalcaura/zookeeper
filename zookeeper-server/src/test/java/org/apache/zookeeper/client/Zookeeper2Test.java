package org.apache.zookeeper.client;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(Parameterized.class)
public class Zookeeper2Test {
    private String path;
    private boolean watch;
    private Stat stat;
    private byte[] data;
    private int version;
    private Type type;

    private ZooKeeper client;
    private String directoryName;
    private int portNumber;
    enum Type{
        EXISTS,
        GET_DATA,
        GET_DATA_NONODE,
        SET_DATA,
        SET_DATA_NONODE,
        SET_DATA_BADVER,
        EXISTS_ILLEGAL_STATE,
        EXISTS_ILLEGAL_ARG,
        EXISTS_KEEPER_EXC,
        SET_DATA_BADARG
    }

    public Zookeeper2Test(Type type, String path, boolean watch, Stat stat, byte[] data, int version) throws IOException, InterruptedException {
        this.directoryName = "zookeeper19";
        this.portNumber = 12367;
        this.type=type;
        this.path = path;
        this.watch = watch;
        this.stat = stat;
        this.data = data;
        this.version = version;
    }

    @Parameterized.Parameters
    public static Collection configure() {
        byte[] data = {};
        return Arrays.asList(new Object[][]{
                {Type.EXISTS,"/test",false,null,new byte[10],-1},
                {Type.EXISTS,"/test",true,null,new byte[2],-1},
                //ILLegalArgum
                {Type.EXISTS_ILLEGAL_ARG,"test",false,null,new byte[2],-1},
                {Type.EXISTS_ILLEGAL_ARG,"",false,null,new byte[2],-1},
                {Type.EXISTS_ILLEGAL_ARG,null,false,null,new byte[2],-1},
                {Type.EXISTS_ILLEGAL_ARG,"/test/../children",false,null,new byte[2],-1},
                {Type.EXISTS_ILLEGAL_STATE,"/test",true,null,data,-1},

                {Type.SET_DATA,"/test",false,null,new byte[10],-1},
                {Type.SET_DATA,"/test",false,null,data,0},
                //{Type.SET_DATA,"/",true,new Stat(),new byte[10],0},
                {Type.SET_DATA_BADVER,"/test",false,null,data,0},
                {Type.SET_DATA_NONODE,"/test",false,null,new byte[2],0},

                {Type.GET_DATA,"/test",false,new Stat(),new byte[10],0},
                {Type.GET_DATA,"/test",true,null,new byte[10],0},
                {Type.GET_DATA_NONODE,"/test",false,null,data,0}
        });
    }

    private ServerCnxnFactory factory;
    private static void cleanDirectory(File dir) {
        if (dir != null && dir.exists()) {
            File[] files = dir.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        cleanDirectory(file); //ricorsivamente pulisce le sottodirectory
                    }
                    if (file.isFile()) {
                        boolean deleted = file.delete();
                        if (deleted) {
                            System.out.println("File deleted: " + file.getAbsolutePath());
                        } else {
                            System.err.println("Unable to delete file: " + file.getAbsolutePath() +
                                    " (Writable: " + file.canWrite() + ")");
                        }
                    }
                }
            }
            boolean dirDeleted = dir.delete(); // Elimina la directory alla fine
            if (dirDeleted && !dir.exists()) {
                System.out.println("Directory " + dir.getAbsolutePath() + " deleted successfully.");
            } else {
                System.err.println("Unable to delete directory: " + dir.getAbsolutePath() +
                        " (Writable: " + dir.canWrite() + ")");
            }
        }
    }

    @Before
    public void createServer() throws IOException, InterruptedException {
        this.client=null;
        System.out.println("Before method");
        int tickTime = 2000;
        int numConnections = 3000;
        String dataDirectory = System.getProperty("java.io.tmpdir");

        String uniqueDirectoryName = directoryName + "_" + UUID.randomUUID();
        this.directoryName = uniqueDirectoryName;
        File dir = new File(dataDirectory, this.directoryName).getAbsoluteFile();
        cleanDirectory(dir);

        ZooKeeperServer server = new ZooKeeperServer(dir, dir, tickTime);

        ServerCnxnFactory standaloneServerFactory = ServerCnxnFactory.createFactory(portNumber, numConnections);
        System.out.println("Factory created");

        int zkPort = standaloneServerFactory.getLocalPort();
        standaloneServerFactory.startup(server);
        System.out.println("Server startup");

        this.factory = standaloneServerFactory;
        String connection = "127.0.0.1:"+ portNumber;

        System.out.println(connection);
        this.client = new ZooKeeper(connection, 2000, event -> {
            //do something with the event processed
        });

    }
    @After
    public void removeServer(){
        System.out.println("After method");
        try {
            if (this.client != null) {
                System.out.println("Closing ZooKeeper client...");
                this.client.close();
            }
            if (this.factory != null) {
                try {
                    ZooKeeperServer server = this.factory.getZooKeeperServer();
                    if (server != null) {
                        System.out.println("Force stopping ZooKeeper server...");
                        server.shutdown();
                    }
                    System.out.println("Shutting down ZooKeeper factory...");
                    this.factory.shutdown();
                } catch (Exception e) {
                    System.err.println("Error during ZooKeeper shutdown: " + e.getMessage());
                    e.printStackTrace();
                }
            }

            Thread.sleep(1000);

            System.out.println("Cleaning directory...");
            String dataDirectory = System.getProperty("java.io.tmpdir");
            File dir = new File(dataDirectory, directoryName).getAbsoluteFile();
            cleanDirectory(dir);
            //dir.delete();
        } catch (Exception e) {
            System.err.println("Error while shutting down ZooKeeper: " + e.getClass().getName() + ": " + e.getMessage());
            e.printStackTrace();
        }

    }



    @Test
    public void testExists()throws KeeperException,InterruptedException{
        Assume.assumeTrue(type==Type.EXISTS);

        byte[] data = {};
        try{
            this.client.create(this.path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }catch(KeeperException.NodeExistsException e){
        }
        Stat stat = this.client.exists(this.path,this.watch);
        Assert.assertEquals(stat.getDataLength(), data.length);
    }

    @Test
    public void testExistsIllegal() throws InterruptedException, KeeperException {
        Assume.assumeTrue(type==Type.EXISTS_ILLEGAL_STATE);

        Exception error = null;
        try{
            this.client.register(null);
            this.client.exists(this.path,true);
        }catch (IllegalStateException e){
            error = e;
        }
        assertNotNull(error);
    }
    @Test
    public void testExistsIllegalArg() throws InterruptedException, KeeperException {
        Assume.assumeTrue(type==Type.EXISTS_ILLEGAL_ARG);
        Exception error = null;
        try{
            this.client.exists(this.path,false);
        }catch (IllegalArgumentException e){
            error = e;
        }
        assertNotNull(error);
    }

    @Test
    public void testSetData() throws InterruptedException, KeeperException {
        Assume.assumeTrue(type == Type.SET_DATA);

        byte[] oldData = {};
        try {
            this.client.create(this.path, oldData, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }catch(KeeperException.NodeExistsException e ){
            this.version = this.client.exists(this.path,false).getVersion();
        }
        Stat stat = this.client.setData(this.path,this.data,this.version);
        Assert.assertEquals(stat.getDataLength(),this.data.length);
    }

    @Test
    public void testSetDataNoNode() throws InterruptedException, KeeperException {
        Assume.assumeTrue(type == Type.SET_DATA_NONODE);
        Exception error = null;
        try{
            this.client.setData(this.path,this.data,this.version);
        }catch (KeeperException.NoNodeException e){
            error = e;
        }
        assertNotNull(error);

    }

    @Test
    public void testSetDataBadVer() throws InterruptedException, KeeperException {
        Assume.assumeTrue(type == Type.SET_DATA_BADVER);

        try {
            this.client.create(this.path, this.data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException e) {
            //ignora se il nodo esiste già
        }

        //recupera la versione attuale del nodo
        int currentVersion = this.client.exists(path, false).getVersion();

        //tenta di impostare i dati con una versione errata
        try {
            this.client.setData(path, data, currentVersion + 1);
            Assert.fail("Expected KeeperException.BadVersionException was not thrown");
        } catch (KeeperException.BadVersionException e) {
            //test riuscito: eccezione prevista è stata generata
            Assert.assertTrue("Caught expected BadVersionException", true);
        }
    }
    @Test
    public void testGetData() throws InterruptedException, KeeperException {
        Assume.assumeTrue(type == Type.GET_DATA);

        try {
            this.client.create(this.path, this.data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException e) {
            int ver = this.client.exists(this.path, false).getVersion();
            this.client.delete(this.path, ver);
            this.client.create(this.path, this.data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        byte[] newData = this.client.getData(this.path, this.watch, this.stat);

        //evito il confronto byte-per-byte se le lunghezze sono diverse
        if (this.data.length != newData.length) {
            Assert.fail("Data lengths do not match.");
        }

        //verifico che i dati siano uguali
        boolean areEquals = true;
        for (int i = 0; i < this.data.length; i++) {
            if (newData[i] != this.data[i]) {
                areEquals = false;
                break;
            }
        }
        Assert.assertTrue(areEquals);
    }
    @Test
    public void getDataNoNodeEx() throws InterruptedException, KeeperException {
        Assume.assumeTrue(type == Type.GET_DATA_NONODE);

        Exception error = null;
        try{
            this.client.getData(this.path,this.watch,this.stat);
        }catch (KeeperException.NoNodeException e){
            error = e;
        }
        assertNotNull(error);
    }
}
