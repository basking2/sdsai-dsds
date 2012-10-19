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
package org.sdsai.dsds.node;

/**
 * A view of {@link Node} objects arranged as a tree with no
 * back links. This will do a depth-first traversal of the tree.
 */
public class NodeLocation<USERKEY, STOREKEY>
{
    private NodeStore<USERKEY, STOREKEY, ?> nodeStore;
    private NodeLocation<USERKEY, STOREKEY> prev;
    private Node<USERKEY, STOREKEY> node;
    private int index;

    public NodeLocation(final NodeStore<USERKEY, STOREKEY, ?> nodeStore,
                        final Node<USERKEY, STOREKEY> node,
                        final int index)
    {
        this(nodeStore, null, node, index);
    }
        
    public NodeLocation(final NodeStore<USERKEY, STOREKEY, ?> nodeStore,
                        final NodeLocation<USERKEY, STOREKEY> prev,
                        final Node<USERKEY, STOREKEY> node,
                        final int index)
    {
        this.nodeStore = nodeStore;
        this.prev = prev;
        this.node = node;
        this.index = index;
    }
    
    public boolean hasPrev()
    {
        if ( index > 1 )
            return true;
    
        return prev!=null;
    }
    
    public boolean hasNext()
    {
        if ( index+1 < node.getChildren().size() ) {
            return true;
        }
        
        return prev!=null;
    }
    
    public Node<USERKEY, STOREKEY> getNode()
    {
        return node;
    }
    
    public NodeLocation<USERKEY, STOREKEY> descend()
    {
        return descend(index);
    }

    public NodeLocation<USERKEY, STOREKEY> descend(final int child)
    {
        final STOREKEY nKey = node.getChildren().get(child);
        final Node<USERKEY, STOREKEY> n = nodeStore.loadNode(nKey);
            
        return new NodeLocation<USERKEY, STOREKEY>(nodeStore, this, n, 0);
    }
    
    /**
     * Return this node with the index positioned on the Right.
     * @return this node with the index positioned on the right.
     */
    public NodeLocation<USERKEY, STOREKEY> right()
    {
        index = node.getChildren().size()-1;
        return this;
    }
    
    /**
     * Return this node with the index positioned on the left.
     * @return this node with the index positioned on the left.
     */
    public NodeLocation<USERKEY, STOREKEY> left()
    {
        index = 0;
        return this;
    }
    
    public NodeLocation<USERKEY, STOREKEY> next()
    {
        NodeLocation<USERKEY, STOREKEY> l = prev;
        
        l.index++;
        
        if ( l.index < l.node.getChildren().size()) {
            l = l.descend().min();
        }

        return l;
    }
        
    public NodeLocation<USERKEY, STOREKEY> prev()
    {
        NodeLocation<USERKEY, STOREKEY> l = prev;
        
        l.index--;
        
        if ( l.index >= 0 ) {
            l = l.descend().max();
        }

        return l;
    }
    
    /**
     * This creates a NodeLocation that is positioned in an imaginary node
     * that exists before the first node in the tree.
     * Used for constructing {@link java.util.Iterator}s 
     * @return a location before the first node in the tree.
     */
    public NodeLocation<USERKEY, STOREKEY> beforeMin()
    {
        return new NodeLocation<USERKEY, STOREKEY>(
            nodeStore,
            min(),
            new Node<USERKEY, STOREKEY>(),
            0);
    }
    
    /**
     * This creates a NodeLocation that is positioned in an imaginary node
     * that exists after the last node in the tree.
     * Used for constructing {@link java.util.Iterator}s 
     * @return a location after the last node in the tree.
     */
    public NodeLocation<USERKEY, STOREKEY> afterMax()
    {
        return new NodeLocation<USERKEY, STOREKEY>(
            nodeStore,
            max(),
            new Node<USERKEY, STOREKEY>(),
            0);
    }
    
    public NodeLocation<USERKEY, STOREKEY> max()
    {
        NodeLocation<USERKEY, STOREKEY> l = right();
        
        while (!l.node.isLeaf()) {
            l = l.descend().right();
        }
        
        return l;
    }
    
    public NodeLocation<USERKEY, STOREKEY> min()
    {
        NodeLocation<USERKEY, STOREKEY> l = left();
        
        while (!l.node.isLeaf()) {
            l = l.descend().left();
        }
        
        return l;
    }
}

