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
package com.github.basking2.sdsai.dsds;

import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.UUID;
import java.util.ArrayList;
import java.util.List;

import static java.util.UUID.randomUUID;

public class BTreeTest extends BaseTest<UUID>
{

    private final Logger logger = LoggerFactory.getLogger(BTreeTest.class);

    @Test
    public void testPutRemoveFew()
    {
            
        final UUID btKey = randomUUID();
        BTree<UUID, File, String> bt =
            new BTree<UUID, File, String>(btKey, nodeStore, 10);

        try
        {
            List<UUID> removeEm = new ArrayList<UUID>(3);
            
            for ( int i = 0; i < 3; i++ ) {
                UUID id = randomUUID();
                removeEm.add(id);
                bt.put(id, "hi");
            }

            for ( final UUID id : removeEm ) {
                bt.remove(id);
            }
        }
        finally
        {
            bt.destroy();
        }
    }
    
    @Test
    public void testPutRemoveThintree() {
            
        final UUID btKey = randomUUID();

        BTree<UUID, File, String> bt = 
            new BTree<UUID, File, String>(btKey, nodeStore, 1);

        try
        {
            List<UUID> removeEm = new ArrayList<UUID>(100);
            
            for ( int i = 0; i < 100; i++ ) {
                UUID id = randomUUID();
                removeEm.add(id);
                bt.put(id, "hi");
            }

            for ( final UUID id : removeEm ) {
                bt.remove(id);
            }

            }
        finally
        {
            bt.destroy();
        }
    }

    @Test
    public void testPutRemove() {
        final UUID btKey = randomUUID();
   
        BTree<UUID, File, String> bt =
            new BTree<UUID, File, String>(btKey, nodeStore, 10);

        try
        {

            List<UUID> removeEm = new ArrayList<UUID>(100);
            
            for ( int i = 0; i < 100; i++ ) {
                UUID id = randomUUID();
                removeEm.add(id);
                bt.put(id, "hi");
            }

            for ( final UUID id : removeEm ) {
                bt.remove(id);
            }
        }
        finally
        {
            bt.destroy();
        }
    }

    @Test
    public void clearTest() {
        final UUID btKey = randomUUID();
        BTree<UUID, File, String> bt =
            new BTree<UUID, File, String>(btKey, nodeStore, 1);

        try
        {
            for ( int i = 0; i < 100; i++ ) {
                bt.put(randomUUID(), "hi");
            }
        }
        finally
        {
            bt.destroy();
        }
    }
    
    
}
