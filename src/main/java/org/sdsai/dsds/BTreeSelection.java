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

import java.util.Iterator;
import java.lang.Iterable;

public class BTreeSelection<USERKEY, STOREKEY>
implements Iterable<USERKEY>
{
    private BTreeLocation<USERKEY, STOREKEY> begin;
    private BTreeLocation<USERKEY, STOREKEY> end;
    
    public BTreeSelection(final BTreeLocation<USERKEY, STOREKEY> begin,
                          final BTreeLocation<USERKEY, STOREKEY> end)
    {
        this.begin = begin;
        this.end = end;
    }
    
    public BTreeSelection(final BTree<USERKEY, STOREKEY, ?> btree,
                          final USERKEY begin,
                          final USERKEY end)
    {
        this(btree.getLocation(begin), btree.getLocation(end));
    }

    @Override
    public Iterator<USERKEY> iterator()
    {
        return new Iterator<USERKEY>()
        {
            private BTreeLocation<USERKEY, STOREKEY> curr =
                new BTreeLocation<USERKEY,STOREKEY>(begin);
            
            public boolean hasNext()
            {
                return curr != null && curr.compareTo(end) < 0;
            }
            
            public USERKEY next()
            {
                final USERKEY k = curr.getKey();
                curr = curr.next();
                return k;
            }
            
            public void remove()
            {
                throw new UnsupportedOperationException();
            }
        };
    }
    
    @Override
    public String toString()
    {
        return begin.getNode() + "\n to \n" + end.getNode();
    }
}
