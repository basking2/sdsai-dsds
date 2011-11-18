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

import org.sdsai.dsds.node.Node;
import org.sdsai.dsds.node.NodeFunction;
import org.sdsai.dsds.node.NodeLocation;
import org.sdsai.dsds.node.NodeStore;
import org.sdsai.dsds.node.NodeStoreNodeNotFoundException;

import org.junit.Test;
import org.junit.Assert;
import org.junit.Ignore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.UUID;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;

import static java.util.UUID.randomUUID;
import static org.junit.Assert.assertEquals;

public class PagedListTest extends BaseTest<File>
{

    private static final Logger logger = 
        LoggerFactory.getLogger(PagedListTest.class);
        
    private void print(final PagedList<File, String> p)
    {
        System.out.println( "--------------------------");
        
        Iterator<Node<File, File>> i = p.pageIterator();
        
        while ( i.hasNext() )
        {
            Node<File,File> n = i.next();
            
            System.out.println("NEXT: "+n.getChildren().get(0));
            System.out.println("PREV: "+n.getAncestors().get(0));
            
            for(final File o : n.getData()) {
                System.out.println("\t"+nodeStore.loadData(o));
            }
        }
        
        System.out.println( "--------------------------");
    }

    @Test
    public void testAddRemove()
    {
        PagedList<File, String> pl = 
            new PagedList<File, String>(nodeStore, nodeStore.generateKey(null), 3);

        try
        {
            for( int i = 0 ; i < 100 ; i++ )
            {
                pl.add(""+i);
            }
            
            // validate the add
            int j = 0;
            for ( String i : pl )
            {
                assertEquals(""+j, i);
                j++;
            }
            

            for( int i = 0 ; i < 100 ; i++ )
            {
                pl.remove(""+i);
            }
            
        }
        finally
        {
            pl.destroy();
        }
    }
    
    @Test
    public void testAddRandomRemove()
    {
        PagedList<File, String> pl = 
            new PagedList<File, String>(nodeStore, nodeStore.generateKey(null), 3);

        try
        {
            for( int i = 0 ; i < 100 ; i++ )
            {
                pl.add(""+i);
            }
            
            // validate the add
            int j = 0;
            for ( String i : pl )
            {
                assertEquals(""+j, i);
                j++;
            }
            

            for( int i = 0 ; i < 100 ; i++ )
            {
                final int randomRemove = (int)(Math.random()*pl.size());
                pl.remove(randomRemove);
            }
            
        }
        finally
        {
            pl.destroy();
        }
    }

    @Test
    public void testAddRandom()
    {
        PagedList<File, String> pl = 
            new PagedList<File, String>(nodeStore, nodeStore.generateKey(null), 3);

        try
        {
            for( int i = 0 ; i < 100 ; i++ )
            {
                final int randomInsert = (int)(Math.random()*pl.size());
                //System.out.println("SZ: "+pl.size());
                //System.out.println("INS: "+randomInsert);
                //print(pl);
                pl.add(randomInsert, ""+i);
            }

            for( int i = 0 ; i < 100 ; i++ )
            {
                final int randomRemove = (int)(Math.random()*pl.size());
                //System.out.println("SZ: "+pl.size());
                //System.out.println("INS: "+randomInsert);
                //print(pl);
                pl.remove(randomRemove);
            }
            
            for ( String i : pl )
            {
                logger.debug("Got {}", i);
            }
        }
        finally
        {
            pl.destroy();
        }
    }
}
