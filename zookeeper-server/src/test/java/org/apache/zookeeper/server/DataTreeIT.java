package org.apache.zookeeper.server;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.StatPersisted;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;

import java.lang.reflect.Field;
import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(Parameterized.class)
public class DataTreeIT {

    private DataTree sut;
    private String path;
    private byte[] data;
    private List<ACL> acl;
    private long ephemeralOwner;
    private int parentCVersion;
    private long zxid;
    private long time;
    public DataTreeIT( String path, byte[] data, List<ACL> acl, long ephemeralOwner, int parentCVersion,
                         long zxid, long time){
        configure( path, data, acl, ephemeralOwner, parentCVersion, zxid, time);
    }

    // Parametri di test
    public void configure( String path, byte[] data, List<ACL> acl, long ephemeralOwner, int parentCVersion,
                          long zxid, long time) {


        this.sut = new DataTree();
        this.path = path;
        this.data = data;
        this.acl = acl;
        this.ephemeralOwner = ephemeralOwner;
        this.parentCVersion = parentCVersion;
        this.zxid = zxid;
        this.time = time;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][] {
                {"/a/b", new byte[1], ZooDefs.Ids.CREATOR_ALL_ACL, 0, 2, 1, 1}
        });
    }
    @Test
    public void testCreateNode() throws NoSuchFieldException, IllegalAccessException, KeeperException.NodeExistsException {

        //setup della struttura dataTree aggiungendo /a
        NodeHashMap nodes = spy(NodeHashMap.class);

        //reflection per impostare la variabile 'nodes' all'interno di 'sut'
        Field field = DataTree.class.getDeclaredField("nodes");
        field.setAccessible(true);  //rendo accessibile il campo privato
        field.set(sut, nodes); //imposto il mock di 'nodes' in 'sut'

        try {
            sut.createNode(path, data, acl, ephemeralOwner, parentCVersion, zxid, time);
        } catch (KeeperException.NoNodeException e) {
            System.out.println("NoNodeException handled as expected");
        }

        //verifico che get sia stato chiamato una volta su nodes
        verify(nodes, times(1)).get(anyString());

    }

}


