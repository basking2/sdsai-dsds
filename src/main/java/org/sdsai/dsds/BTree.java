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

import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.CancellationException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import static java.util.Collections.binarySearch;

/**
 * <p>A B-Tree that uses a {@link NodeStore} object to persist {@link Node}s
 * and user data to blob storage. This is tailored to NoSQL 
 * document / key-value systems.</p>
 *
 * <p>There is no concurrency built into this object so edits to a BTree must
 * be coordinated through a locking mechanism or some otherwise atomic 
 * service. Developers looking to add transactionality should consider
 * implementing a {@link NodeStore} to collect and atomically batch-update
 * edits.</p>
 *
 * @param K The user key type. See {@link Map} for its use.
 * @param STOREKEY The storage key that K will be transformed in to.
 *        The {@link NodeStore} must be able to convert a K to a STOREKEY
 *        and must be able to generate a STOREKEY for storing internal nodes.
 * @param V The user value type. Sett {@link Map} for its use.
 */
public class BTree<K,STOREKEY, V> implements Map<K,V>
{
    /**
     * The key to store and retrieve the root key.
     */
    private STOREKEY rootKey;
    
    /**
     * The storage engine.
     */
    private NodeStore<K, STOREKEY, V> nodeStore;
    
    /**
     * The minimum data stored in an internal node. Per the B-Tree definition
     * the root node is the exception to this rule. It may have less.
     */
    private int minData;
    
    /**
     * The index of the first element split into a left node. 0.
     * Used in node splitting. Set by {@link #updateIndexes()}.
     */
    private int leftDataStart;
    
    /**
     * The exclusive index of the last element split into a left node.
     * Used in node splitting. Set by {@link #updateIndexes()}.
     */
    private int leftDataEnd;
    
    /**
     * The index of the first element split into a left node. 0.
     * Used in node splitting. Set by {@link #updateIndexes()}.
     */
    private int leftChildStart;
    
    /**
     * The exclusive index of the last element split into a left node.
     * Used in node splitting. Set by {@link #updateIndexes()}.
     */
    private int leftChildEnd;
    
    /**
     * The index of the center data index.
     * Used in node splitting. Set by {@link #updateIndexes()}.
     */
    private int middleData;
    
    /**
     * The index of the first element split into a right node.
     * Used in node splitting. Set by {@link #updateIndexes()}.
     */
    private int rightDataStart;
    
    /**
     * The exclusive index of the last element split into a right node.
     * Used in node splitting. Set by {@link #updateIndexes()}.
     */
    private int rightDataEnd;
    
    /**
     * The index of the first element split into a right node.
     * Used in node splitting. Set by {@link #updateIndexes()}.
     */
    private int rightChildStart;
    
    /**
     * The exclusive index of the last element split into a right node.
     * Used in node splitting. Set by {@link #updateIndexes()}.
     */
    private int rightChildEnd;

    /**
     * @param rootKey
     * @param nodeStore
     */
    public BTree(final K rootKey, final NodeStore<K, STOREKEY, V> nodeStore)
    {
        this(rootKey, nodeStore, 100);
    }
    
    /**
     * <p>Attempt to retrieve the root node stored at the {@code rootKey}
     * or, if none is found, construct a new empty root node
     * with {@code minData*2} as the size of the {@code maxData} value to the
     * {@link Node}'s constructors.</p>
     * 
     * <p>The {@code rootKey} will never be altered where as internal nodes
     *    are not guaranteed to have the same key.</p>
     *
     * @param rootKey The key at which the BTree's root is stored.
     * @param nodeStore The storage engine.
     * @param minData The minimum amount of data that this BTree will store
     *        in a {@link Node}. The maximum data will be {@code minData*2+1}
     *        and the number of child {@link Node}s will be one more than that. 
     */
    public BTree(final K rootKey,
                 final NodeStore<K, STOREKEY, V> nodeStore,
                 final int minData)
    {
        this.nodeStore = nodeStore;
        this.rootKey = nodeStore.convert(rootKey);
        this.minData = minData;
        updateIndexes();
    }
    
    /**
     * Load the root node into this BTree from {@link NodeStore} or,
     * if no node is found, create it.
     *
     * @throws NodeStoreException
     */
    private Node<K, STOREKEY> getRoot()
    {
        Node<K, STOREKEY> root;
        
        try {
            root = nodeStore.loadNode(rootKey);
            this.minData = root.getDataCap() / 2;
            updateIndexes();
        } catch ( final NodeStoreNodeNotFoundException e) {
            root = newNode();
            nodeStore.store(rootKey, root);
        }
        
        return root;
    }
    
    /**
     * Any time minData is updated this is called to 
     * recompute where a Node is split.
     */
    private void updateIndexes() {
        this.leftDataStart = 0;
        this.leftDataEnd = minData;
        this.leftChildStart = 0;
        this.leftChildEnd = minData+1;
        this.middleData = minData;
        this.rightDataStart = minData+1;
        this.rightDataEnd = 2*minData+1;
        this.rightChildStart = minData+1;
        this.rightChildEnd = 2*minData+2;
    }
    
    /**
     * Check the {@link NodeStore} for the given key.
     * @return true of the key is found in the {@link NodeStore}.
     */
    @Override
    public boolean containsKey(Object key) { 
        return null != nodeStore.loadData(nodeStore.convert((K)key));
    }
    
    /**
     * Clears the datastructure by doing a depth-first node traversal
     * to delete all the user data and the node data from 
     * the {@link NodeStore}. This uses {@link #eachDepthFirst}
     * to visit each node. Notice that this will <em>not</em> delete
     * the root {@link Node} key as other pieces of code may
     * be accessing this object.
     */
    @Override
    public void clear() {
    
        eachDepthFirst(new NodeFunction<K,STOREKEY>(){
            public boolean call(final Node<K, STOREKEY> n) {
                for (final K k : n.getData()) {
                    nodeStore.removeData(nodeStore.convert(k));
                }
                
                for (final STOREKEY k : n.getChildren()) {
                    nodeStore.removeNode(k);
                }
                
                return true;
            }
        });
    }
    
    /**
     * Whereas clearing will destroy all but the root node, 
     * this method calls {@link #clear()} and deletes the root node key.
     */
    public void destroy()
    {
        clear();
        nodeStore.removeNode(rootKey);
    }
    
    /**
     * Recrusively depth-first execute 
     * {@link NodeFunction#call(Node)} on each. This is used by
     * {@link #clear} to destroy the datastructure in an orderly way.
     * This is the same order provided by {@link #getNodeIterator}
     * and {@link NodeLocation}.
     * 
     */
    public void eachDepthFirst(final NodeFunction<K,STOREKEY> callable)
    {
        eachDepthFirst(callable, getRoot());
    }
    
    /**
     * A private helper function for 
     * {@link #eachDepthFirst(final NodeFunction<K, STOREKEY> callable)}
     */
    private boolean eachDepthFirst(final NodeFunction<K,STOREKEY> callable,
                                final Node<K, STOREKEY> root)
    {
        for (final STOREKEY k : root.getChildren()) {
            if ( ! eachDepthFirst(callable, nodeStore.loadNode(k)) )
                return false;
        }
        
        return callable.call(root);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean containsValue(Object value) {
        
        if ( value == null ) {
            return false;
        }

        final Iterator<K> itr = getIterator();
        
        while (itr.hasNext()) {
            final K k = itr.next();
            if ( value.equals( nodeStore.loadData(nodeStore.convert(k)) ) ) {
                return true;
            }
        }
        
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<Map.Entry<K,V>> entrySet(){
        return new AbstractSet<Map.Entry<K,V>>(){
        
            @Override
            public Iterator<Map.Entry<K,V>> iterator() {
                return new Iterator<Map.Entry<K,V>>() {
                    final Iterator<K> iterator = BTree.this.getIterator();
                    @Override
                    public boolean hasNext()
                    {
                        return iterator.hasNext();
                    }
                    @Override
                    public Map.Entry<K,V> next()
                    {
                        return new Map.Entry<K,V>()
                        {
                            final private K key = iterator.next();
                            private V value = null;
                            
                            @Override
                            public boolean equals(Object o)
                            {
                                if ( o == null )
                                    return false;
                                    
                                Map.Entry<K,V> m2 = (Map.Entry<K,V>) o;
                                
                                return 
                                    (this.getKey()==null?
                                        m2.getKey()==null
                                        :
                                        this.getKey().equals(m2.getKey())) 
                                    &&
                                    (this.getValue()==null?
                                        m2.getValue() == null
                                        :
                                        this.getValue().equals(m2.getValue()));
                                    
                            }
                            
                            @Override
                            public K getKey()
                            {
                                return key;
                            }
                            
                            @Override
                            public V getValue()
                            {
                                if ( value == null )
                                {
                                    value = nodeStore.loadData(
                                        nodeStore.convert(key));
                                }
                                
                                return value;
                            }
                            
                            @Override
                            public int hashCode(){
                                return key.hashCode();
                            }
                            
                            @Override
                            public V setValue(final V value)
                            {
                                this.value = value;
                                return BTree.this.put(key, value);
                            }
                        };
                    }
                    @Override
                    public void remove()
                    {
                        throw new UnsupportedOperationException();
                    }
                };
            }
            
            public int size() {
                return BTree.this.size();
            }
        };
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o) {
        if ( o == null )
            return false;
            
        if ( ! ( o instanceof BTree ) )
            return false;
            
        final BTree that = (BTree) o;
        
        return this.rootKey.equals(that.rootKey);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Set<K> keySet() { 
        return new AbstractSet<K>(){
            @Override
            public Iterator<K> iterator() {
                return BTree.this.getIterator();
            }
            
            public int size() {
                return BTree.this.size();
            }
        };
    }

    /**
     * Retrieve the object from the {@link NodeStore}.
     * @return The user data returned by {@link NodeStore#loadData(Object)}.
     */
    @Override
    public V get(Object key){
        return nodeStore.loadData(nodeStore.convert((K)key));
    }

    /**
     * Return the hash code of the {@link #rootKey} used to create this tree.
     * @return the hash code of the {@link #rootKey} used to create this tree.
     */
    @Override
    public int hashCode(){
        return rootKey.hashCode();
    }

    /**
     * Return true if the fetched root node has no data in it.
     * @return true if the fetched root node has no data in it.
     */
    @Override
    public boolean isEmpty(){
        return getRoot().getData().isEmpty();
    }
    
    /**
     * Returns a node constructed for use in a BTree.
     * Specifically {@code child = minData*2+2} and 
     * {@code data = minData*2+1} and {@code ancestors = 0}.
     *
     * @return a node constructed for use in a BTree.
     */
    private Node<K, STOREKEY> newNode()
    {
        // child, data, ancestors
        return new Node<K, STOREKEY>(minData*2+2, minData*2+1, 0);
    }
    
    /**
     * Put the value into the BTree stored in the {@link NodeStore}.
     * This first checks for the object in {@link NodeStore}. If it already
     * exists then no BTree structural change is made and the object
     * is replaced. The old value is returned.
     *
     * @param key the user key which will be converted to a STOREKEY
     *            by {@link NodeStore#convert(Object)}.
     * @param value The value stored.
     */
    @Override
    public V put(final K key, final V value)
    {
        final STOREKEY storeKey = nodeStore.convert(key);
        
        // If the key is already in the store, we are only replacing it.
        // We have hard work to do only when a new key is added.
        if ( containsKey(key) ) {
            final V v = get(key);
            nodeStore.store(storeKey, value);
            return v;
        }
        
        // A new key is being added.
        final NodeContext ctx = new NodeContext();
        int insertionPoint = -1;
        
        while ( ! ctx.node.isLeaf() ) {

            ctx.conditionallySplit(insertionPoint, key, storeKey);
            
            // the index of the search key, if it is contained in the list ;
            //   otherwise, (-(insertion point) - 1)
            final int index = binarySearch(ctx.node.getData(), key, null);
            
            insertionPoint = -(index+1);
            
            ctx.descend(insertionPoint);
        }
        
        // We've stepped into a leaf.
        ctx.conditionallySplit(insertionPoint, key, storeKey);

        final int index = binarySearch(ctx.node.getData(), key, null);
        
        insertionPoint = -(index+1);
        
        ctx.node.getData().add(insertionPoint, key);
        nodeStore.store(storeKey, value);
        nodeStore.store(ctx.nodeKey, ctx.node);
    
        // We've already determined that there is not already a value.
        // So this is always null.
        return null; 
    }
    
    /**
     * Calls {@link Map#entrySet()} and {@link #put}s each elment.
     */
    @Override
    public void putAll(Map<? extends K,? extends V> m) {
        for(final Map.Entry<? extends K,? extends V> me : m.entrySet()) {
            put(me.getKey(), me.getValue());
        }
    }
    
    /**
     * Removes the object from the {@link NodeStore} and restructures
     * the BTree.
     *
     * @return the object stored at {@code keyObject} in 
     * the {@link NodeStore}.
     *
     * @throws ClassCastException if {@code keyObject} is not a key.
     */
    @Override
    public V remove(Object keyObject) {
        final K userKey = (K) keyObject;
        final STOREKEY storeKey = nodeStore.convert(userKey);
        
        final V v = nodeStore.loadData(storeKey);

        if ( v == null ) {
            return null;
        }
        
        final NodeContext ctx = new NodeContext();

        // Notice we never conditionallyCollapse the root node.

        //System.out.println("----------------"+storeKey);

        boolean deleted = false;
        
        // Calls to internalNodeDelete can force us to continue searching
        // by setting deleted to false.
        while ( ! deleted ) {
        
            // Find the key (or -(insertionPoint+1)).
            // This is called once to initialize and once if internalNodeDelete
            // return false.
            int index = binarySearch(ctx.node.getData(), userKey, null);
        
            // While the key is in a child node.
            while ( index < 0 ) {
            
                //System.out.println("-----------------------------"+ctx.node);
                //System.out.println("--|"+index);

                final int insertionPoint = -(index+1);
                
                ctx.descend(insertionPoint);

                // Now the insetionPoint is really the parentInsertionPoint.
                // Pass it to the conditionallyCollapse method.
                ctx.conditionallyCollapse(insertionPoint);
                
                // Setup for the next iteration. Did we find a node with the key?
                index = binarySearch(ctx.node.getData(), userKey, null);
            }
            
            //System.out.println("----------------------------+"+ctx.node);
            // When here the index is positive and is the location of the key.

            if ( ctx.node.isLeaf() ) {
                ctx.node.getData().remove(index);
                nodeStore.store(ctx.nodeKey, ctx.node);
                nodeStore.removeData(storeKey);
                deleted = true;
            } else {
                deleted = internalNodeDelete(ctx, storeKey, index);
            }
        }
        
        return v;
    }
    
    /**
     * <p>
     * This is called when the {@code storeKey} is found by {@link #remove}
     * and it is not located in a leaf node. This method will first
     * try to replace the key with the maximum key from the left child node
     * or the minimum key from the right child node. If both child nodes
     * do not have an extra data key (incase they must be collapsed)
     * then the two children are merged. If this occurs the {@code storeKey}
     * is not deleted and {@code false} is returned to signal
     * the calling {@link #remove} method to coninue searching to remove
     * the key.</p>
     *
     * <p>Eventually the key will be deleted either when there is an
     * available extra key in a child node or when a leaf node is reached.</p>
     *
     * @return false if the node was key was not deleted but was moved
     *         into a child node by a collapsing of two nodes.
     */
    private boolean internalNodeDelete(final NodeContext ctx,
                                       final STOREKEY storeKey,
                                       final int index)
    {
        final STOREKEY leftChildKey = ctx.node.getChildren().get(index);
        final STOREKEY rightChildKey = ctx.node.getChildren().get(index+1);
        final Node<K, STOREKEY> leftChild = nodeStore.loadNode(leftChildKey);
        final Node<K, STOREKEY> rightChild = nodeStore.loadNode(rightChildKey);
        
        if ( leftChild.getData().size() == minData ) {
            if ( rightChild.getData().size() == minData ) {
            
                leftChild.getData().add(ctx.node.getData().remove(index));
                leftChild.getData().addAll(rightChild.getData());
                
                ctx.node.getChildren().remove(index+1);
                
                if (! leftChild.isLeaf())
                    leftChild.getChildren().addAll(rightChild.getChildren());

                nodeStore.store(ctx.nodeKey, ctx.node);
                nodeStore.store(leftChildKey, leftChild);
                nodeStore.removeNode(rightChildKey);
                
                return false;
            } else {
                final NodeContext rctx = new NodeContext(ctx.nodeKey,
                                                         ctx.node,
                                                         rightChildKey,
                                                         rightChild);
                final K replacement = detachMin(rctx);
                ctx.node.getData().set(index, replacement);
                nodeStore.store(ctx.nodeKey, ctx.node);
                nodeStore.removeData(storeKey);
            }
        } else {
            final NodeContext lctx = new NodeContext(ctx.nodeKey,
                                                     ctx.node,
                                                     leftChildKey,
                                                     leftChild);
            final K replacement = detachMax(lctx);
            ctx.node.getData().set(index, replacement);
            nodeStore.store(ctx.nodeKey, ctx.node);
            nodeStore.removeData(storeKey);
        }
        
        return true;
    }
    
    /**
     * Remove the maximum (right-most) data key
     * from the subtree denoted by this node context.
     * The data is not deleted from the {@link NodeStore}.
     */
    private K detachMax(final NodeContext ctx)
    {
        while (!ctx.node.isLeaf())
        {
            int index = ctx.node.getChildren().size()-1;
            ctx.descend(index);
            ctx.conditionallyCollapse(index);
        }
        
        final K key = ctx
            .node
            .getData()
            .remove(ctx.node.getData().size()-1);
        
        nodeStore.store(ctx.nodeKey, ctx.node);
        
        return key;
    }

    /**
     * Remove the minimum (left-most) data key
     * from the subtree denoted by this node context.
     * The data is not deleted from the {@link NodeStore}.
     */
    private K detachMin(final NodeContext ctx)
    {
        while (!ctx.node.isLeaf())
        {
            ctx.descend(0);
            ctx.conditionallyCollapse(0);
        }
        
        final K key = ctx.node.getData().remove(0);
        
        nodeStore.store(ctx.nodeKey, ctx.node);
        
        return key;
    }
    
    /**
     * Iterate through the entire datastrucuture and counts each element.
     * Avoid using this.
     */
    @Override
    public int size() {
        final Integer[] i = new Integer[1];
        i[0] = 0;
        
        eachDepthFirst(new NodeFunction<K, STOREKEY>(){
            @Override
            public boolean call(final Node<K, STOREKEY> n) {
                i[0] += n.getData().size();
                return true;
            }
        } );
        
        return i[0];
    }
    
    /**
     * Iterate through then entire datastructure and retrieves each
     * element.
     */
    @Override
    public Collection<V> values(){
        return new AbstractCollection<V>(){
            @Override
            public Iterator<V> iterator() {
                return new Iterator<V>() {
                    final Iterator<K> iterator = BTree.this.getIterator();
                    @Override
                    public boolean hasNext()
                    {
                        return iterator.hasNext();
                    }
                    @Override
                    public V next()
                    {
                        return nodeStore.loadData(
                            nodeStore.convert(iterator.next()));
                    }
                    @Override
                    public void remove()
                    {
                        throw new UnsupportedOperationException();
                    }
                };
            }
            
            public int size() {
                return BTree.this.size();
            }
        };
    }
    
    /**
     * A private class to hold the current node and its parent node.
     * This also defines a few useful utility methods.
     * This is used during calls to {@link #put} and {@link #remove}.
     */
    private class NodeContext {
        public STOREKEY nodeKey;
        public Node<K, STOREKEY> node;
        public STOREKEY parentKey;
        public Node<K, STOREKEY> parent;
        
        /**
         * Construct a new NodeContext that has a null parent
         * and a node pointing to the root of the {@link BTree}.
         */
        public NodeContext() {
            this(null, null, rootKey, getRoot());
        }
        
        /**
         * Consturct a NodeContext with the given parent and node values.
         *
         * @param parentKey the STOREKEY of the {@code parent}.
         * @param parent the {@link Node<K, STOREKEY>} of {@code node}.
         * @param nodeKey The STOREKEY that points to {@code node}.
         * @param node The {@link Node<K, STOREKEY>}.
         */
        public NodeContext(final STOREKEY parentKey,
                           final Node<K, STOREKEY> parent,
                           final STOREKEY nodeKey,
                           final Node<K, STOREKEY> node)
        {
            this.nodeKey = nodeKey;
            this.node = node;
            this.parentKey = parentKey;
            this.parent = parent;
        }
        
        /**
         * If {@code node} is not null and has the minimum data lements in it
         * it will steal a key and node subtree from a neighbor and
         * {@link #parent} <em>or</em> it will be collapsed with
         * one of its siblings into a single node.
         * It is requried that the parent node has an extra data element
         * to allow for margin two child nodes.
         */
        public boolean conditionallyCollapse(final int parentInsertionPoint)
        {
            // This node is not the root and
            // does not have minData+1 or more data elements.
            // It needs another data element to ensure a child delete can
            // occur.
            if ( ! atRoot() && node.getData().size() == minData )
            {
                //System.out.println("Collapse "+parentInsertionPoint);

                // Data to determine if we steal or merge. We prefer merging.
                STOREKEY leftSiblingKey = null;
                STOREKEY rightSiblingKey = null;
                Node<K, STOREKEY> leftSibling = null;
                Node<K, STOREKEY> rightSibling = null;
                int leftSiblingDataSize = -1;
                int rightSiblingDataSize = -1;
            
                if ( parentInsertionPoint > 0 ) {
                    leftSiblingKey = parent
                        .getChildren()
                        .get(parentInsertionPoint-1);
                    leftSibling = nodeStore.loadNode(leftSiblingKey);
                    leftSiblingDataSize = leftSibling.getData().size();
                }

                if ( parentInsertionPoint < parent.getChildren().size()-1 ) {
                    rightSiblingKey = parent
                        .getChildren()
                        .get(parentInsertionPoint+1);
                    rightSibling = nodeStore.loadNode(rightSiblingKey);
                    rightSiblingDataSize = rightSibling.getData().size();
                }
                
                // Check for a merge opportunity. Avoids a log-n delete min/max.
                if ( leftSiblingDataSize == minData ) {
                    // Collapse left.
                    // Add data to left sibling.
                    if ( ! node.isLeaf() )
                        leftSibling.getChildren().addAll(node.getChildren());
                    leftSibling.getData().add(
                        parent.getData().remove(parentInsertionPoint-1));
                    leftSibling.getData().addAll(node.getData());
                    
                    // remove node and data from parent.
                    parent.getChildren().remove(parentInsertionPoint);
                    nodeStore.removeNode(nodeKey);
                    
                    // node is now inside the left sibling, so replace node.
                    node = leftSibling;

                    // If data == 0, the parent node must be collapsed.
                    if ( parent.getData().size() == 0 ) {
                        nodeStore.removeNode(leftSiblingKey);
                        // Note, the root key must always be static.
                        nodeKey = parentKey;
                        parent = null;
                        parentKey = null;
                    } else {
                        nodeKey = leftSiblingKey;
                        nodeStore.store(parentKey, parent);
                    }
                    
                    nodeStore.store(nodeKey, node);
                    
                } else if ( rightSiblingDataSize == minData ) {
                    // Collapse the right node into node.
                    // Add data to node.
                    if ( ! rightSibling.isLeaf() )
                        node.getChildren().addAll(rightSibling.getChildren());
                    node.getData().add(
                        parent.getData().remove(parentInsertionPoint));
                    node.getData().addAll(rightSibling.getData());
                    
                    // Remove right sibling.
                    parent.getChildren().remove(parentInsertionPoint+1);
                    nodeStore.removeNode(rightSiblingKey);
                    
                    if ( parent.getData().size() == 0 ) {
                        nodeStore.removeNode(nodeKey);
                        // Note, the root key must always be static.
                        nodeKey = parentKey;
                        parent = null;
                        parentKey = null;
                    } else {
                        nodeStore.store(parentKey, parent);
                    }
                    
                    // write results
                    nodeStore.store(nodeKey, node);
                    
                    // NOTE: We do not reassign node because the right
                    // sibling is collapsed into the current node.
                } else if ( leftSiblingDataSize > minData ) {
                    if ( ! leftSibling.isLeaf() ) {
                        final STOREKEY leftChild = 
                            leftSibling.getChildren().remove(
                                leftSibling.getChildren().size()-1);
                        node.getChildren().add(0, leftChild);
                    }

                    final K leftData = leftSibling.getData().remove(
                        leftSibling.getData().size()-1);
                    final K parentData = parent.getData().set(
                        parentInsertionPoint-1, leftData);
                    node.getData().add(0, parentData);

                    // store results
                    nodeStore.store(nodeKey, node);
                    nodeStore.store(leftSiblingKey, leftSibling);
                    nodeStore.store(parentKey, parent);
                } else if ( rightSiblingDataSize > minData ) {
                    if ( ! rightSibling.isLeaf() ) {
                        final STOREKEY rightChild = rightSibling
                            .getChildren()
                            .remove(0);
                        node.getChildren().add(rightChild);
                    }

                    final K rightData = rightSibling
                        .getData()
                        .remove(0);
                    final K parentData = parent.getData().set(
                        parentInsertionPoint, rightData);
                    node.getData().add(parentData);

                    // store results
                    nodeStore.store(nodeKey, node);
                    nodeStore.store(rightSiblingKey, rightSibling);
                    nodeStore.store(parentKey, parent);
                }
                
                return true;
            }
            
            return false;
        }
        
        /**
         * <p>Split the node if {@link Node#isDataFull()} is true.
         *
         * @return true if the node was split.
         */
        public boolean conditionallySplit(
            final int parentInsertionPoint,
            final K userKey,
            final STOREKEY searchKey)
        {
            if ( node.isDataFull() ) {
                //System.out.println("Split "+parentInsertionPoint);
            
                final Node<K, STOREKEY> left = newNode();
                final Node<K, STOREKEY> right = newNode();

                right.getData().addAll(
                    node.getData().subList(rightDataStart, rightDataEnd));
                left.getData().addAll(
                    node.getData().subList(leftDataStart, leftDataEnd));
                    
                assert(left.getData().size() == minData);
                assert(right.getData().size() == minData);
                
                if ( ! node.isLeaf() ) {
                    left.getChildren().addAll(
                        node.getChildren().subList(
                            leftChildStart, leftChildEnd));
                    right.getChildren().addAll(
                        node.getChildren().subList(
                            rightChildStart, rightChildEnd));
                        
                    assert(left.getChildren().size() == minData+1);
                    assert(right.getChildren().size() == minData+1);
                }
                
                final STOREKEY leftKey = nodeStore.generateKey(left);
                final STOREKEY rightKey = nodeStore.generateKey(right);
                final K dataKey = node.getData().get(middleData);
                                    
                if ( atRoot() ) {
                    //System.out.println("SPLIT ROOT.");
                    // restructure the node.
                    node.getData().clear();
                    node.getChildren().clear();
                    node.getData().add(dataKey);
                    node.getChildren().add(leftKey);
                    node.getChildren().add(rightKey);
                    
                    // We will exit this split in a child node. Set the parent.
                    parent = node;
                    parentKey = nodeKey;
                    
                } else {
                    //System.out.println("SPLIT NON_ROOT.");
                    parent.getData().add(parentInsertionPoint, dataKey);
                    parent.getChildren().set(parentInsertionPoint, rightKey);
                    parent.getChildren().add(parentInsertionPoint, leftKey);

                    // Parent isn't changing. However, this node is gone.
                    nodeStore.removeNode(nodeKey);
                }
                
                // Store the changes.
                nodeStore.store(parentKey, parent);
                nodeStore.store(rightKey, right);
                nodeStore.store(leftKey, left);
                
                // pick the left or right child as the current node to be in.
                if ( ( (Comparable)dataKey).compareTo(userKey) <= 0 ) {
                    node = right;
                    nodeKey = rightKey;
                } else {
                    node = left;
                    nodeKey = leftKey;
                }
                return true;
            }
            
            return false;
        }
        
        /**
         * Return true if this context is positioned at the root node.
         * @return true if this context is positioned at the root node.
         */
        public boolean atRoot()
        {
            return parent == null;
        }
        
        /**
         * Step this context into a child node. This updates the parent
         * and node information.
         */      
        public void descend(final int i)
        {
            parentKey = nodeKey;
            parent = node;
            //System.out.println("I="+i);
            nodeKey = node.getChildren().get(i);
            node = nodeStore.loadNode(nodeKey);
        }
    }
    
    public BTreeLocation<K,STOREKEY> getStart()
    {
        BTreeLocation l = new BTreeLocation(nodeStore, getRoot(), 0).min();

        l.index = -1;
        l.setSubtreeHasNext();
        l.setSubtreeHasPrev();

        return l;
    }
    
    public BTreeLocation<K ,STOREKEY> getEnd()
    {
        BTreeLocation l = new BTreeLocation(nodeStore, getRoot(), 0).max();

        l.index = l.node.getData().size();
        l.setSubtreeHasNext();
        l.setSubtreeHasPrev();

        return l;
    }

    /**
     * Return a {@link BTreeLocation} wrapped in an iterator.
     * This will call {@link BTreeLocation#hasNext()} and 
     * {@link BTreeLocation#next()} to iterate through this BTree.
     */
    public Iterator<K> getIterator()
    {
        return new Iterator<K>()
        {
            private BTreeLocation<K, STOREKEY> nextState = getStart();
    
            public boolean hasNext()
            {
                return nextState != null && nextState.hasNext();
            }
            
            public K next()
            {
                nextState = nextState.next();
                
                if ( nextState == null )
                    throw new NoSuchElementException();
                
                return nextState.getKey();
            }
            
            /**
             * Unsupported operation.
             * @throws UnsupportedOperationException
             */
            public void remove()
            {
                throw new UnsupportedOperationException();
            }
        };
    }
    
    /**
     * Return a {@link BTreeLocation} wrapped in an iterator.
     * This will call {@link BTreeLocation#prev()} and 
     * {@link BTreeLocation#hasPrev()} to iterator through the BTree
     * in reverse.
     */
    public Iterator<K> getReverseIterator()
    {
        return new Iterator<K>()
        {
            private BTreeLocation<K, STOREKEY> prevState = getEnd();
    
            public boolean hasNext()
            {
                return prevState != null && prevState.hasPrev();
            }
            
            public K next()
            {
                prevState = prevState.prev();
                
                if ( prevState == null )
                    throw new NoSuchElementException();
                
                return prevState.getKey();
            }
            
            /**
             * Unsupported operation.
             * @throws UnsupportedOperationException
             */
            public void remove()
            {
                throw new UnsupportedOperationException();
            }
        };
    }
    
    public NodeLocation<K, STOREKEY> getStartNode()
    {
        return new NodeLocation(nodeStore, getRoot(), 0).beforeMin();
    }

    public NodeLocation<K, STOREKEY> getEndNode()
    {
        return new NodeLocation(nodeStore, getRoot(), 0).afterMax();
    }

    public Iterator<Node<K, STOREKEY>> getNodeIterator()
    {
        return new Iterator<Node<K, STOREKEY>>()
        {
            private NodeLocation<K, STOREKEY> nextState = getStartNode();
    
            public boolean hasNext()
            {
                return nextState != null && nextState.hasNext();
            }
            
            public Node<K, STOREKEY> next()
            {
                nextState = nextState.next();
                
                if ( nextState == null )
                    throw new NoSuchElementException();
                
                return nextState.getNode();
            }
            
            /**
             * Unsupported operation.
             * @throws UnsupportedOperationException
             */
            public void remove()
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    public Iterator<Node<K, STOREKEY>> getReverseNodeIterator()
    {
        return new Iterator<Node<K, STOREKEY>>()
        {
            private NodeLocation<K, STOREKEY> nextState = getEndNode();
    
            public boolean hasNext()
            {
                return nextState != null && nextState.hasPrev();
            }
            
            public Node<K, STOREKEY> next()
            {
                nextState = nextState.prev();
                
                if ( nextState == null )
                    throw new NoSuchElementException();
                
                return nextState.getNode();
            }
            
            /**
             * Unsupported operation.
             * @throws UnsupportedOperationException
             */
            public void remove()
            {
                throw new UnsupportedOperationException();
            }
        };
    }
    
    public BTreeLocation<K, STOREKEY> getLocation(final K key)
    {
        BTreeLocation<K, STOREKEY> loc = 
            new BTreeLocation<K, STOREKEY>(nodeStore, getRoot(), 0);
        
        int index = binarySearch(loc.node.getData(), key, null);

        // This is always overwritten before it is used.
        int insertionPoint = 0;
        
        while ( index < 0 && ! loc.node.isLeaf() ) {
            insertionPoint = -(index+1);
            
            loc = loc.go(insertionPoint).descend();

            index = binarySearch(loc.node.getData(), key, null);
        }
        
        insertionPoint = -(index+1);
        
        // If index > 0, we found the exact key. Return.
        return loc.go( (index >= 0 ? index : insertionPoint) );
    }

    /**
     * Select an inclusive range from lower to upper.
     *
     * @param lower The lower boundary of the selection, inclusive.
     * @param upper The upper boundary of the selection, exclusive.
     */
    public BTreeSelection<K,STOREKEY> select(final K lower, final K upper)
    {
        return new BTreeSelection<K, STOREKEY>(
            getLocation(lower), getLocation(upper));
    }
}
