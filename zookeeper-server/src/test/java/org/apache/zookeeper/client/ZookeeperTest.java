package org.apache.zookeeper.client;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class ZookeeperTest {

    private ZooKeeper client;
    private String directoryName;
    private int portNumber;
    private String path;
    private byte[] data;
    private List<ACL> acl;
    private CreateMode createMode;
    private Type type;
    private int version;
    private enum Type{
        CREATE_NoNodeEXC,
        CREATE_NODE,
        CREATE_ILLEGAL_ARG_EXC,
        CREATE_NODEEXISTS,
        CREATE_KEEPEREF,
        CREATE_INVALID_ACL_EXC,
        DELETE_NO_NODE_EXC,
        DELETE_BADVERS_EXC,
        DELETE_KEEPERNOEMPTY,
        DELETE_NODE,
        DELETE_ILLEGAL_ARG_EXC,
        GET_EPHEMERALS
    }
    private ServerCnxnFactory factory;
    private static void cleanDirectory(File dir) {
        if (dir != null && dir.exists()) {
            File[] files = dir.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        cleanDirectory(file); // Ricorsivamente pulisce le sottodirectory
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

    public ZookeeperTest(Type type, String path, byte[] data, ArrayList<ACL> acl, CreateMode createMode, int version) throws IOException {
        this.directoryName = "zookeeper";
        this.portNumber = 12349;
        this.type = type;
        this.path = path;
        this.data = data;
        this.acl = acl;
        this.createMode = createMode;
        this.version = version;
    }
    @Parameterized.Parameters
    public static Collection configure(){
        byte[] data = {};
        return Arrays.asList(new Object[][]{
                {Type.CREATE_NODE, "/test", new byte[10], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, 0},
                {Type.CREATE_INVALID_ACL_EXC, "/test", new byte[10], new ArrayList<ACL>(), CreateMode.EPHEMERAL, 0},
                {Type.CREATE_INVALID_ACL_EXC, "/test", new byte[10], null, CreateMode.EPHEMERAL, 0},
                { Type.CREATE_INVALID_ACL_EXC, "/test", data, new ArrayList<ACL>(), CreateMode.EPHEMERAL, 0},
                {Type.CREATE_NODEEXISTS, "/", new byte[10], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, 0},
                {Type.CREATE_INVALID_ACL_EXC, "/test", data, null, CreateMode.EPHEMERAL,  0},
                {Type.CREATE_NODE, "/test", data, ZooDefs.Ids.READ_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL, 0},
                {Type.CREATE_NoNodeEXC, "/testfail/no/parent", data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, 0},
                {Type.CREATE_NoNodeEXC, "/testfail/no/parent", new byte[2], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, 0},
                {Type.CREATE_ILLEGAL_ARG_EXC, "a", data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, 0},
                //dovrebbe fallire perchè path sbagliato ma mi sa che va a buon fine
                {Type.CREATE_ILLEGAL_ARG_EXC, "/a/../c", new byte[2], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, 0},
                {Type.CREATE_KEEPEREF, "/testeph", data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, 0},
                {Type.CREATE_KEEPEREF, "/testeph", new byte[2], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, 0},

                {Type.DELETE_NO_NODE_EXC, "/test_no_node",data,ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT,0},
                {Type.DELETE_BADVERS_EXC,"/test_bad_version",data,ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT,1},
                {Type.DELETE_BADVERS_EXC,"/test_bad_version2",data,ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT,-2},
                {Type.DELETE_KEEPERNOEMPTY,"/test_delete_children",data,ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT,0},
                {Type.DELETE_NODE, "/test_delete",data,ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT,0},
                //dovrebe fallire ma va a buon fine
                {Type.DELETE_ILLEGAL_ARG_EXC, "/../test_delete_illegal",data,ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT,0},


        });

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
    public void testCreate() {
        Assume.assumeTrue(type == Type.CREATE_NODE);
        Exception error = null;
        try{
            this.client.create(this.path,this.data,this.acl, this.createMode);

        }catch (Exception e){
            error = e;
        }
        assertNull(error);
    }

    @Test
    public void testCreateInvalidACL() throws InterruptedException, KeeperException {
        //if the ACL is invalid, null, or empty
        Assume.assumeTrue(type==Type.CREATE_INVALID_ACL_EXC);

        Exception error = null;

        try{
            this.client.create(this.path,this.data,this.acl,this.createMode);
        }catch (KeeperException.InvalidACLException e){
            error = e;
        }
        assertNotNull(error);
    }

    @Test
    public void testCreateNoNodeEx() throws InterruptedException, KeeperException {
        //if the parent node does not exist in the ZooKeeper
        Assume.assumeTrue(type==Type.CREATE_NoNodeEXC);
        Exception error = null;

        try{
            this.client.create(this.path,this.data, this.acl,this.createMode);
        }catch (KeeperException.NoNodeException e){
            error = e;
        }
        assertNotNull(error);

    }
    @Test
    public void testCreateNodeExistsException() throws InterruptedException, KeeperException {
        Assume.assumeTrue(type==Type.CREATE_NODEEXISTS);

        Exception error = null;
        try{
            this.client.create(this.path,this.data,this.acl, this.createMode);
            this.client.create(this.path,this.data,this.acl, this.createMode);
        }catch (KeeperException.NodeExistsException e){
            error = e;
        }
        assertNotNull(error);
    }

    @Test
    public void testCreateKeeperExEphemeral() throws InterruptedException, KeeperException {
        Assume.assumeTrue(type == Type.CREATE_KEEPEREF);

        byte[] parentData = {};
        String pathParent;
        Exception error = null;

        try {
            pathParent = this.client.create("/ephemeral_parent1", parentData, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (KeeperException.NodeExistsException e) {
            pathParent = "/ephemeral_parent1";
        }

        try {

            this.client.create(pathParent + this.path, this.data, this.acl, this.createMode);
        } catch (KeeperException.NoChildrenForEphemeralsException e) {
            error = e;
        }
        assertNotNull(error);
    }

    @Test
    public void testCreateIllegalArgEx() throws InterruptedException, KeeperException {
        //invalid path is specified
        Assume.assumeTrue(type == Type.CREATE_ILLEGAL_ARG_EXC);

        Exception error = null;

        try{
            this.client.create(this.path,this.data, this.acl,this.createMode);
        }catch (IllegalArgumentException e){
            error = e;
        }
        assertNotNull(error);
    }

    @Test
    public void testDeleteNoNode() {
        Assume.assumeTrue(type == Type.DELETE_NO_NODE_EXC);
        Exception error = null;
        try{
            this.client.delete(this.path, this.version);

        }catch (Exception e){
            error = e;
            System.out.println("error: "+ e);
        }
        assertNotNull(error);
    }

    @Test
    public void testDeleteBadVersion() throws InterruptedException, KeeperException {
        Assume.assumeTrue(type == Type.DELETE_BADVERS_EXC);
        Exception error = null;

        try{
            this.client.create(this.path, this.data, this.acl, this.createMode);
            this.client.delete(this.path, this.version);

        }catch (KeeperException.BadVersionException e){
            error = e;
        }
        assertNotNull(error);
    }

    @Test
    public void testDeleteWithChildren() throws InterruptedException, KeeperException {
        Assume.assumeTrue(type == Type.DELETE_KEEPERNOEMPTY);

        boolean childrenCreated = false;
        Exception error = null;

        try {

            this.client.create(this.path, this.data, this.acl, this.createMode);
            this.client.create(this.path + "/new_children", this.data, this.acl, this.createMode);

            childrenCreated = true;

        } catch (KeeperException.NodeExistsException e) {
            if (!childrenCreated) {
                try {

                    this.client.create(this.path + "/new_children", this.data, this.acl, this.createMode);
                } catch (KeeperException.NodeExistsException e1) {

                }
            } else {

                this.version = this.client.exists(this.path + "/new_children", false).getVersion();
            }
        }

        try {

            this.client.delete(this.path, this.version);
        } catch (KeeperException.NotEmptyException e) {
            error = e;
        }

        assertNotNull(error);
        assertTrue(error instanceof KeeperException.NotEmptyException);
    }

    @Test
    public void testDelete() throws  InterruptedException, KeeperException{
        Assume.assumeTrue(type == Type.DELETE_NODE);
        try{
            this.client.create(this.path,this.data,this.acl,this.createMode);
        }catch(KeeperException.NodeExistsException e){
            this.version = this.client.exists(this.path,false).getVersion();
        }
        this.client.delete(this.path,this.version);
        Stat nodeStat = this.client.exists(this.path,false);
        Assert.assertNull(nodeStat);
    }
    @Test
    public void testDeleteIllegalArg() throws InterruptedException, KeeperException {
        Assume.assumeTrue(type == Type.DELETE_ILLEGAL_ARG_EXC);
        Exception error = null;

        try{
            this.client.delete(this.path, this.version);

        }catch (IllegalArgumentException e){
            error = e;
        }
        assertNotNull(error);
    }

}
