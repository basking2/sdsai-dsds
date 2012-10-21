/**
 * Copyright (c) 2011, Samuel R. Baskinger <basking2@yahoo.com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a 
 * copy  of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation 
 * the rights to use, copy, modify, merge, publish, distribute, sublicense, 
 * and/or sell copies of the Software, and to permit persons to whom the 
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included 
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS 
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING 
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER 
 * DEALINGS IN THE SOFTWARE.
 */
package org.sdsai.dsds;

import org.sdsai.dsds.fs.DirectoryNodeStore;

import org.sdsai.dsds.node.Node;
import org.sdsai.dsds.node.NodeFunction;
import org.sdsai.dsds.node.NodeLocation;
import org.sdsai.dsds.node.NodeStore;
import org.sdsai.dsds.node.NodeStoreNodeNotFoundException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.UUID;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;

import static java.util.UUID.randomUUID;
import static java.util.Collections.reverse;
import static java.util.Collections.sort;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class BTreeIteratorTest extends BaseTest<UUID>
{
    private List<UUID> idList;
    private List<File> fileList;
    
    @Before
    public void setup()
    {
        idList = new ArrayList<UUID>(100);
        fileList = new ArrayList<File>(100);
                
        for ( int i = 0; i < 100; i++ ) {
            final UUID id = randomUUID();
            fileList.add(nodeStore.convert(id));
            idList.add(id);
        }
    }

    @Test
    public void shallowTreeForwardIteration()
    {
        final UUID btId = randomUUID();
    
        BTree<UUID, File, String> bt =
            new BTree<UUID, File, String>(btId, nodeStore, 10);

        fwdIteration(bt);

        nodeStore.removeNode(nodeStore.convert(btId));
    }

    @Test
    public void deepTreeForwardIteration()
    {
        final UUID btId = randomUUID();
    
        BTree<UUID, File, String> bt =
            new BTree<UUID, File, String>(btId, nodeStore, 1);

        fwdIteration(bt);

        nodeStore.removeNode(nodeStore.convert(btId));
    }

    @Test
    public void shallowTreeBackwardsIteration()
    {
        final UUID btId = randomUUID();
    
        BTree<UUID, File, String> bt =
            new BTree<UUID, File, String>(btId, nodeStore, 10);

        bkwdIteration(bt);

        nodeStore.removeNode(nodeStore.convert(btId));
    }
    
    @Test
    public void deepTreeBackwardsIteration()
    {
        final UUID btId   = randomUUID();
    
        BTree<UUID, File, String> bt =
            new BTree<UUID, File, String>(btId, nodeStore, 1);

        bkwdIteration(bt);

        nodeStore.removeNode(nodeStore.convert(btId));
    }

    private void fwdIteration(final BTree<UUID, File, String> bt)
    {

        for( final UUID id : idList ) {
            bt.put(id, "hi "+id);
        }
        
        
        Iterator<UUID> bti = bt.getIterator();
        
        UUID k = bti.next();
        
        int count = 1;

        sort(idList);
        
        while ( bti.hasNext() ) {
            final UUID tmp = bti.next();
            assertTrue( k.compareTo(tmp) <= 0 );
            assertEquals(idList.get(count-1), k);
            assertEquals(idList.get(count), tmp);
            k = tmp;
            count++;
        }
        
        assertEquals(100, count);

        for ( final UUID id : idList ) {
            bt.remove(id);
        }

    }

    private void bkwdIteration(final BTree<UUID, File, String> bt)
    {
        for( final UUID id : idList ) {
            bt.put(id, "hi "+id);
        }
        
        Iterator<UUID> bti = bt.getReverseIterator();
        
        UUID k = bti.next();
        
        int count = 1;

        sort(idList);
        reverse(idList);
        
        while ( bti.hasNext() ) {
            final UUID tmp = bti.next();
            assertTrue( k.compareTo(tmp) >= 0 );
            assertEquals(idList.get(count-1), k);
            assertEquals(idList.get(count), tmp);
            k = tmp;
            count++;
        }
        
        assertEquals(100, count);

        for ( final UUID id : idList ) {
            bt.remove(id);
        }
    }
    
    @Test
    public void forwardsBackwardsTest()
    {
        final NodeStore<Integer, File, String> ns = 
            new DirectoryNodeStore<Integer, String>("target/forwadsBackwardsTest");
            
        final Integer btId = 0;
        
        BTree<Integer, File, String> bt =
            new BTree<Integer, File, String>(btId, ns, 1);

        bt.put(1, "hi 1");
        bt.put(5, "hi 5");
        bt.put(2, "hi 2");
        bt.put(3, "hi 3");
        bt.put(4, "hi 4");
        
        BTreeLocation<Integer,File> loc = bt.getStart();

        loc = loc.next();
        assertEquals(Integer.valueOf(1), loc.getKey());
        loc = loc.next();
        assertEquals(Integer.valueOf(2), loc.getKey());
        loc = loc.next();
        assertEquals(Integer.valueOf(3), loc.getKey());
        loc = loc.next();
        assertEquals(Integer.valueOf(4), loc.getKey());
        loc = loc.next();
        assertEquals(Integer.valueOf(5), loc.getKey());
        assertNull(loc.next());
        loc = loc.prev();
        assertEquals(Integer.valueOf(4), loc.getKey());
        loc = loc.prev();
        assertEquals(Integer.valueOf(3), loc.getKey());
        loc = loc.prev();
        assertEquals(Integer.valueOf(2), loc.getKey());
        loc = loc.prev();
        assertEquals(Integer.valueOf(1), loc.getKey());
        assertNull(loc.prev());
        
        bt.clear();
        ns.removeNode(ns.convert(btId));
    }

}
