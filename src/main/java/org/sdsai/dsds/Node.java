package org.sdsai.dsds;

import org.sdsai.Key;

import java.lang.reflect.Array;
import java.io.Serializable;

/**
 * The basic object that is persisted to a large distributed 
 * storage via a {@link NodeStore}. Notice that a {@link Node}
 * does not posses any knowlede of the key it is stored under.
 * Only it's children or ancestor nodes.
 */
public class Node<K> implements Serializable
{
    private Object[] data;
    private Object[] ancestors;
    private Object[] children;
    
    private int dataFill;
    private int ancestorsFill;
    private int childrenFill;

    /**
     * Create a Node that can store a single ancestor, child, and data object.
     */
    public Node() {
        this(1,1,1);
    }
    
    public Node(final int maxData,
                final int maxChildren,
                final int maxAncestors)
    {
        data = new Object[maxData];
        children = new Object[maxChildren];
        ancestors = new Object[maxAncestors];
        
        dataFill = 0;
        ancestorsFill = 0;
        childrenFill = 0;
    }
    
    public K[] getData() {
        return (K[]) data;
    }
    
    public K[] getChildren() {
        return (K[]) children;
    }
    
    public K[] getAncestors() {
        return (K[]) ancestors;
    }
    
    public void setData(final K[] data) {
        this.data = data;
    }
    
    public void setChildren(final K[] children) {
        this.children = children;
    }
    
    public void setAncestors(final K[] ancestors) {
        this.ancestors = ancestors;
    }
    
    public K getData(final int i) {
        return (K) data[i];
    }

    public void setData(final int i, final K d) {
        data[i] = d;
    }

    public K swapData(final int i, final K d) {
        final K tmp = (K) data[i];
        data[i] = d;
        return tmp;
    }
    
    public K getChild(final int i) {
        return (K) children[i];
    }
    
    public void setChild(final int i, final K k) {
        children[i] = k;
    }
    
    public K swapChild(final int i, final K k) {
        final K tmp = (K) children[i];
        children[i] = k;
        return tmp;
    }
    
    public K getAncestor(final int i) {
        return (K) ancestors[i];
    }
    
    public K swapAncestor(final int i, final K key) {
        final K tmp = (K) ancestors[i];
        ancestors[i] = key;
        return tmp;
    }
    
    public void setAncestor(final int i, final K k) {
        ancestors[i] = k;
    }
} // public class Node
