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
import org.sdsai.dsds.node.Node;


/**
 * A recursive reprentation of a point in an in-order traversal of a B-Tree.
 */
public class BTreeLocation<USERKEY,STOREKEY>
implements Comparable<BTreeLocation<USERKEY, STOREKEY>>
{

    /**
     * The previous state (not the previous tree node).
     * This the state that we left the parent node in.
     */
    public BTreeLocation<USERKEY,STOREKEY> prev;
    public Node<USERKEY,STOREKEY> node;
    
    /**
     */
    public int index;
    public NodeStore<USERKEY, STOREKEY, ?> nodeStore;
    public boolean subtreeHasNext;
    public boolean subtreeHasPrev;
    
    public BTreeLocation(final NodeStore<USERKEY, STOREKEY, ?> nodeStore,
                         final Node<USERKEY, STOREKEY> node,
                         final int index)
    {
        this.nodeStore = nodeStore;
        this.node = node;
        this.index = index;
        setSubtreeHasNext();
        setSubtreeHasPrev();
    }
    
    public BTreeLocation(final BTreeLocation<USERKEY,STOREKEY> prev,
                         final Node<USERKEY, STOREKEY> node,
                         final int index)
    {
        this.nodeStore = prev.nodeStore;
        this.prev = prev;
        this.node = node;
        this.index = index;
        setSubtreeHasNext();
        setSubtreeHasPrev();
    }
    
    /**
     * Copy constructor.
     */
    public BTreeLocation(
        final BTreeLocation<USERKEY, STOREKEY> loc)
    {
        this.nodeStore = loc.nodeStore;
        this.prev = loc.prev;
        this.node = loc.node;
        this.index = loc.index;
        this.subtreeHasPrev = loc.subtreeHasPrev;
        this.subtreeHasNext = loc.subtreeHasNext;
    }

    public int compareTo(final BTreeLocation<USERKEY, STOREKEY> that)
    {
        if ( that == null )
            return 1;
            
        USERKEY thisKey;
        USERKEY thatKey;
        
        int thisSz = this.node.getData().size();
        int thatSz = that.node.getData().size();

        if ( thisSz == 0 && thatSz > 0 )
            return 1;

        if ( thisSz > 0 && thatSz == 0 )
            return -1;
            
        if ( thisSz == 0 && thatSz == 0 )
            return 0;
        
        // If here, both nodes have data to compare.
        
        if ( this.index < 0 )
            thisKey = this.node.getData().get(0);
        else if ( this.index >= thisSz )
            thisKey = this.node.getData().get(thisSz-1);
        else
            thisKey = this.node.getData().get(this.index);
        
        if ( that.index < 0 )
            thatKey = that.node.getData().get(0);
        else if ( that.index >= thatSz )
            thatKey = that.node.getData().get(thatSz-1);
        else
            thatKey = that.node.getData().get(that.index);

        return ((Comparable)thisKey).compareTo(thatKey);
    }
        
    public boolean hasPrev()
    {
        if ( subtreeHasPrev ) {
            return true;
        } else if ( prev != null ) {
            return prev.hasPrev();
        } else {
            return false;
        }
    }
    
    public boolean hasNext()
    {
        if ( subtreeHasNext ) {
            return true;
        } else if ( prev != null ) {
            return prev.hasNext();
        } else {
            return false;
        }
    }
    
    /**
     * May never be called unless {@link #hasNext()} is true.
     * Sets {@link #hasPrev()} to true.
     */
    public BTreeLocation<USERKEY,STOREKEY> next()
    {
        // Step forwards. Now return either this state or the min child.
        if (subtreeHasNext) {
            go(index+1);
        
            if (node.isLeaf()) {
                return this;
            } else {

                BTreeLocation<USERKEY,STOREKEY> l = descend(index).leftChild();
                
                while (!l.node.isLeaf()) {
                    l = l.leftChild().descend().leftData();
                }
                
                return l;
            }
        } else {
            return walkUpTreeUntilHasNext();
        }
    }

    // code for walking up a tree to go prev.
    private BTreeLocation<USERKEY,STOREKEY> walkUpTreeUntilHasPrev() {
        BTreeLocation<USERKEY,STOREKEY> l = prev;
        
        while (l!=null && !l.subtreeHasPrev)
            l = l.prev;
        
        // We know this is true because we were in a subtree.
        if ( l != null ) {
            l.subtreeHasNext = true;

            // This block handles conditions where
            // we have changed iteration direction (prev to next, 
            // next to prev) and need to guard against situations
            // where we have iterated off the edge of a parent node.
            if ( l.index == l.node.getData().size() ) {
                l.index--;
                l.setSubtreeHasPrev();
                l.setSubtreeHasNext();
            }
        }
        
          return l;
    }
   
    // code for walking up a tree to go next
    private BTreeLocation<USERKEY,STOREKEY> walkUpTreeUntilHasNext() {
        BTreeLocation<USERKEY,STOREKEY> l = prev;
      
        // Walk up the subtrees until nodes are available.
        while ( l != null &&  !l.subtreeHasNext)
            l = l.prev;
            
        // We know this is true because we were IN a subtree.
        if ( l != null ) {
            l.subtreeHasPrev = true;

            // This block handles conditions where
            // we have changed iteration direction (prev to next, 
            // next to prev) and need to guard against situations
            // where we have iterated off the edge of a parent node.
            if ( l.index == -1 ) {
                go(index+1);
            }
        }
          
        return l;
    }

    public BTreeLocation<USERKEY,STOREKEY> prev()
    {
        if (subtreeHasPrev) {
            go(index-1);
            
            if (node.isLeaf()) {
                return this;
            } else {

                BTreeLocation<USERKEY,STOREKEY> l = descend(index+1).rightData();
                
                while (!l.node.isLeaf()) {
                    l = l.rightChild().descend().rightData();
                }
                
                return l;
            }
        } else {
            return walkUpTreeUntilHasPrev();
        }
    }
    
    
    /**
     * Set hasNext using local information about if there
     * is another available datalement in this subtree.
     * This does not take into account the parent's hasNext status.
     */
    public void setSubtreeHasNext()
    {
        if (index+1 < node.getData().size()) {
            // There is another data element.
            subtreeHasNext = true;
        } else if (index+1 < node.getChildren().size()) {
            // There is another subtree.
            subtreeHasNext = true;
        } else {
            // We're out of options.
            subtreeHasNext = false;
        }
    }
    
    /**
     * Set hasPrev using local node information only.
     */
    public void setSubtreeHasPrev()
    {
        if ( index > 0 ) {
            // There is a node we can get to.
            subtreeHasPrev = true;
        } else if ( ! node.isLeaf() && index > -1) {
            // There is a left child we can get to.
            subtreeHasPrev = true;
        } else {
            subtreeHasPrev = false;
        }
    }

    /**
     * Return the current STOREKEY to the user data.
     */
    public USERKEY getKey()
    {
        return node.getData().get(index);
    }
    
    /**
     * Return the current Node.
     */
    public Node<USERKEY,STOREKEY> getNode()
    {
        return node;
    }
    
    public BTreeLocation<USERKEY, STOREKEY> leftChild()
    {
        index = 0;
        setSubtreeHasNext();
        setSubtreeHasPrev();
        return this;
    }
    
    public BTreeLocation<USERKEY, STOREKEY> leftData()
    {
        return leftChild();
    }        

    public BTreeLocation<USERKEY, STOREKEY> rightChild()
    {
        return go(node.getChildren().size()-1);
    }

    public BTreeLocation<USERKEY, STOREKEY> rightData()
    {
        return go(node.getData().size()-1);
    }
    
    /**
     * Set the index to this value and return this.
     */
    public BTreeLocation<USERKEY, STOREKEY> go(final int index)
    {
        this.index = index;
        setSubtreeHasNext();
        setSubtreeHasPrev();
        return this;
    }
    
    /**
     * Push the current state and step into the given child.
     */
    public BTreeLocation<USERKEY, STOREKEY> descend(final int child)
    {
        final Node<USERKEY, STOREKEY> nextNode = nodeStore
            .loadNode(node.getChildren().get(child));
            
        return new BTreeLocation<USERKEY, STOREKEY>(this, nextNode, 0);
    }
    
    public BTreeLocation<USERKEY, STOREKEY> descend()
    {
        return descend(index);
    }

    /**
     * Return the minimum location for this subtree location.
     * 
     * @return the minimum location for this subtree location.
     */
    public BTreeLocation<USERKEY,STOREKEY> min()
    {
        BTreeLocation<USERKEY,STOREKEY> l = leftChild();

        while (!l.node.isLeaf()) {
            l = l.leftChild().descend().leftData();
        }
        
        return l;
    }
    
    /**
     * Return the maximum location for this subtree location.
     * 
     * @return the maximum location for this subtree location.
     */
    public BTreeLocation<USERKEY,STOREKEY> max()
    {
        BTreeLocation<USERKEY,STOREKEY> l = rightChild();
        
        while (!l.node.isLeaf()) {
            l = l.rightChild().descend().rightData();
        }
        
        return l;
    }
    
    
}
