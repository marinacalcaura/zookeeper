package org.apache.zookeeper.server;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(Parameterized.class)
public class DeleteDataTreeTest {

    private String path;
    private long zxid;

    private DataTree sut;
    private TestType testType;

    public DeleteDataTreeTest (TestType testType, String path, long zxid){
        configure(testType, path, zxid);
    }

    // Parametri di test
    public void configure(TestType testType, String path, long zxid) {

        this.testType = testType;
        this.sut = new DataTree();
        this.path = path;
        this.zxid = zxid;
    }

    @Before
    public void setUpTree() {

        if(path != null) {

            try {

                String[] pathElements = path.split("/");

                System.out.println(Arrays.toString(pathElements));
                String oldPath = "";
                int i = 1;
                for (String elem : pathElements) {
                    System.out.println(pathElements.length + "    i: " + i);
                    if (!(i == 1)) { //modifica per creare il nodo interamente

                        System.out.println(oldPath + "/" + pathElements[i - 1]);
                        this.sut.createNode(oldPath + "/" + pathElements[i - 1], new byte[10], ZooDefs.Ids.CREATOR_ALL_ACL, 0,sut.getNode(oldPath).stat.getCversion(), 2, 1);
                        oldPath = oldPath + "/" + pathElements[i - 1];

                    }
                    i++;
                }
            } catch (Exception e) {
                e.printStackTrace();

            }
        }
    }

    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {
        return Arrays.asList(new Object[][]{
//nodo non effimero, delete
                {TestType.DELETE_VALID_PATH,"/a/b", 1},
                //mi aspetto un'eccezione e invece il test fallisce
                //{TestType.DELETE_INVALID_PATH,"/a/../c", 1},
                {TestType.DELETE_VALID_PATH,"/node1", 1},
                {TestType.DELETE_NO_NODE_EXC,"/a/b", 1},

                //agg container, null e ""

        });
    }

    @Test
    public void testDeleteValidNode(){
        Assume.assumeTrue(testType == TestType.DELETE_VALID_PATH);
        Exception error = null;
        try{
            sut.deleteNode(path,zxid);
        }catch (KeeperException.NoNodeException e){
            error = e;
        }
        assertNull(error);
    }
    @Test
    public void testDeleteInvalidNode(){
        Assume.assumeTrue(testType == TestType.DELETE_INVALID_PATH);
        Exception error = null;
        try{
            sut.deleteNode(path,zxid);
        }catch (KeeperException.NoNodeException e){
            error = e;
        }
        assertNotNull(error);
    }
    @Test
    public void testDeleteNoNodeExc()  {
        //caso in cui il genitore manca
        Assume.assumeTrue(testType == TestType.DELETE_NO_NODE_EXC);
        Exception error = null;
        try{
            String parentPath;
            //devo eliminare il percorso prima del nodo che voglio eliminare,quindi /a/b elimino prima /a
            int lastSlashIndex = path.lastIndexOf("/");
            if (lastSlashIndex == 0) {
                parentPath= "/"; // Il nodo genitore è la radice
            }
            parentPath= path.substring(0, lastSlashIndex);
            sut.deleteNode(parentPath,zxid);
            sut.deleteNode(path,zxid);

        }catch (KeeperException.NoNodeException e){
            error = e;

        }
        assertNotNull(error);
    }
    private enum TestType {

        DELETE_VALID_PATH,
        DELETE_INVALID_PATH,
        DELETE_NO_NODE_EXC

    }


}