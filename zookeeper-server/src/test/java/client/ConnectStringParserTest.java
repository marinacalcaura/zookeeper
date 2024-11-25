package client;

import org.apache.zookeeper.client.ConnectStringParser;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ConnectStringParserTest {

    @Test
    public void testGetChrootPath() {
        // Test senza chroot path
        ConnectStringParser parser = new ConnectStringParser("127.0.0.1:2181");
        assertNull(parser.getChrootPath());

        // Test con chroot path
        ConnectStringParser parserWithChroot = new ConnectStringParser("127.0.0.1:2181/testpath");
        assertEquals("/testpath", parserWithChroot.getChrootPath());
    }

}
