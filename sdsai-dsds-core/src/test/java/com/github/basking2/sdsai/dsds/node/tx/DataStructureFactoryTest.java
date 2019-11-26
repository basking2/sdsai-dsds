package com.github.basking2.sdsai.dsds.node.tx;

import java.util.UUID;
import java.util.Map;
import java.util.List;

import com.github.basking2.sdsai.dsds.node.NodeStore;
import com.github.basking2.sdsai.dsds.fs.DirectoryNodeStore;
import java.io.File;
import java.io.Serializable;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.UUID.randomUUID;
import static org.junit.Assert.assertEquals;

public class DataStructureFactoryTest
{
    private final Logger logger = LoggerFactory.getLogger(DataStructureFactoryTest.class);

    private static <USERKEY extends Serializable> NodeStore<USERKEY, File, String> buildNodeStore(final Class<USERKEY> clazz)
    {
        final NodeStore<USERKEY, File, String> nodeStore =
            new DirectoryNodeStore<USERKEY, String>("target/DirectoryNodeStore/"+DataStructureFactoryTest.class.getSimpleName());

        return nodeStore;
    }

    @Test
    public void testTxBTree()
    {
        final UUID btKey = randomUUID();
        final Map<UUID, String> map = DataStructureFactory.bTree(btKey, buildNodeStore(UUID.class));
    }

    @Test
    public void testTxPagedList()
    {
        final NodeStore<File, File, String> nodeStore = buildNodeStore(File.class);

        final List<String> list = DataStructureFactory.pagedList(nodeStore.generateKey(null, null), nodeStore, 3);

        for(int i = 0; i < 100; i++)
        {
            list.add("Hi "+i);
        }

        for(int i = 0; i < 100; i++)
        {
            final String s = "Hi "+i;
            assertEquals(s, list.get(i));
        }

        list.clear();
    }
}
