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

import com.github.basking2.sdsai.dsds.node.Node;
import com.github.basking2.sdsai.dsds.node.NodeStore;

/**
 * This is mostly a wrapper around {@link Node}
 * but also includes some meta information about
 * where in the page of data represented by a Node in 
 * PagedList.
 */
public class PagedListLocation<STOREKEY>
{
    private NodeStore<STOREKEY, STOREKEY, ?> nodeStore;
    private Node<STOREKEY, STOREKEY> node;
    private STOREKEY key;
    private int index;
    
    public PagedListLocation(final NodeStore<STOREKEY, STOREKEY, ?> nodeStore,
                             final STOREKEY key,
                             final Node<STOREKEY, STOREKEY> node,
                             final int index)
    {
        this.nodeStore = nodeStore;
        this.key = key;
        this.node = node;
        this.index = index;
    }
    
    public PagedListLocation(final NodeStore<STOREKEY, STOREKEY, ?> nodeStore,
                             final STOREKEY key,
                             final Node<STOREKEY, STOREKEY> node)
    {
        this(nodeStore, key, node, 0);
    }
    
    public PagedListLocation(final NodeStore<STOREKEY, STOREKEY, ?> nodeStore, final STOREKEY key)
    {
        this(nodeStore, key, nodeStore.loadNode(key));
    }
    
    public STOREKEY nextKey()
    {
        return node.getChildren().get(0);
    }
    
    public STOREKEY prevKey()
    {
        return node.getAncestors().get(0);
    }
    
    public PagedListLocation<STOREKEY> next()
    {
        return new PagedListLocation<STOREKEY>(nodeStore, nextKey());
    }
    
    public PagedListLocation<STOREKEY> prev()
    {
        return new PagedListLocation<STOREKEY>(nodeStore, prevKey());
    }
    
    /**
     * Set the index / position of this PagedListLocation
     * and return this.
     *
     * @return this
     */
    public PagedListLocation<STOREKEY> index(final int index)
    {
        this.index = index;
        return this;
    }
    
    public int getIndex()
    {
        return index;
    }
    
    /**
     * Return the data element stored in the node at {@link #getIndex()}.
     */
    public STOREKEY getData()
    {
        return node.getData().get(index);
    }
    
    public Node<STOREKEY, STOREKEY> getNode()
    {
        return node;
    }

    public STOREKEY getKey()
    {
        return key;
    }
    
    /**
     * Set the key at which this node is stored.
     */
    public void setKey(final STOREKEY key)
    {
        this.key = key;
    }
    
    /**
     * Calls store on the nodeStore with the current key and node.
     */
    public void store()
    {
        nodeStore.store(key, node);
    }
    
    /**
     * Calls store on the nodeStore with the current key and node.
     */
    public void remove()
    {
        nodeStore.removeNode(key);
    }
    
    public int size()
    {
        return node.getData().size();
    }
}
