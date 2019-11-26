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
import java.util.Iterator;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;

public class BTreeSelectionTest extends BaseTest<Integer>
{

    private final Logger logger = 
        LoggerFactory.getLogger(BTreeSelectionTest.class);

    @Test
    public void test001() 
    {
            
        BTree<Integer, File, String> bt =
            new BTree<Integer, File, String>(1, nodeStore, 1);

        try 
        {
            for ( int i = 0; i < 100; i++ ) {
                bt.put(10000+i, "hi");
            }
            
            Iterator<Integer> i = bt.select(10003, 10004).iterator();

            assertTrue(i.hasNext());
            assertEquals( (Integer)10003, i.next());
            assertFalse(i.hasNext());

        }
        finally
        {
            bt.destroy();
        }
    }

    @Test
    public void test002() 
    {
            
        BTree<Integer, File, String> bt =
            new BTree<Integer, File, String>(2, nodeStore, 1);

        try 
        {

            bt.put(10001, "hi");
            bt.put(10002, "hi");
            bt.put(10004, "hi");
            bt.put(10006, "hi");
            bt.put(10009, "hi");
            
            Iterator<Integer> i = bt.select(10003, 10005).iterator();
            
            assertTrue(i.hasNext());
            assertEquals( (Integer)10004, i.next());
            assertFalse(i.hasNext());

        }
        finally
        {
            bt.destroy();
        }
    }    
}
