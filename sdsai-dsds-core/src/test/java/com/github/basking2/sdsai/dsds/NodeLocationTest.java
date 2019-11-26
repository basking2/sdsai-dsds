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
import java.util.ArrayList;
import java.util.List;

import com.github.basking2.sdsai.dsds.node.Node;
import com.github.basking2.sdsai.dsds.node.NodeFunction;
import com.github.basking2.sdsai.dsds.node.NodeLocation;

import static org.junit.Assert.assertEquals;

public class NodeLocationTest extends BaseTest<Integer>
{

    private final Logger logger = LoggerFactory.getLogger(NodeLocationTest.class);

    @Test
    public void testLocation10()
    {
        testLocation(10);
    }
    
    @Test
    public void testLocation100()
    {
        testLocation(100);
    }
    
    public void testLocation(final int sz)
    {
            
        final Integer btKey = 10000000;

        final BTree<Integer, File, String> bt =
            new BTree<Integer, File, String>(btKey, nodeStore, 1);
        try
        {
            List<Integer> l = new ArrayList<Integer>();
            
            for ( int i = 0; i < sz; i++ ) {
                Integer id = 100+i;
                l.add(id);
                bt.put(id, "hi"+i);
            }

            int nodeCount1 = 0;
            int nodeCount2 = 0;
            
            NodeLocation<Integer, File> nodeLocation = bt.getStartNode();
            
            while( nodeLocation.hasNext() ) {
                nodeLocation = nodeLocation.next();
                nodeCount1++;
            }

            nodeLocation = bt.getEndNode();
            
            while( nodeLocation.hasPrev() ) {
                nodeLocation = nodeLocation.prev();
                nodeCount2++;
            }
            
            final int[] btSize = new int[]{ 0 };
            
            bt.eachDepthFirst(new NodeFunction<Integer,File>(){
                @Override
                public boolean call(final Node<Integer, File> n) {
                    btSize[0]++;
                    return true;
                }
            });
            
            
            assertEquals(sz, bt.size());
            
            assertEquals("Forward count does not work.", btSize[0], nodeCount1);
            assertEquals("Backwards count does not work.", btSize[0], nodeCount2);
        }
        finally
        {
            bt.destroy();
        }
    }
   
}
