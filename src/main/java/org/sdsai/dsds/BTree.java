package org.sdsai.dsds;

import org.sdsai.Key;

import java.util.Map;
import java.util.Set;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.CancellationException;

public class BTree<K,V> implements Map<K,V>
{
    private K rootKey;
    private Node<K> root;
    private NodeStore<K, V> nodeStore;
    private int minData;
    
    public BTree(final K rootKey, final NodeStore<K,V> nodeStore)
    {
        this(rootKey, nodeStore, 100);
    }
    /**
     * Attempt to retrieve the root node stored at the {@code rootKey}
     * or, if none is found, construct a new empty root node
     * with {@code minData*2} as the size of the {@code maxData} value to the
     * {@link Node}'s constructors.
     */
    public BTree(final K rootKey,
                 final NodeStore<K,V> nodeStore,
                 final int minData)
    {
        this.nodeStore = nodeStore;
        this.rootKey = rootKey;
        this.minData = minData;
    }
    
    public Node<K,V> getRoot() throws NodeStoreException
    {
        try
        {
            root = nodeStore.loadNode(rootKey).get();
            
            if ( root == null ) {
                root = new Node<K>(minData*2, minData*2+1, 1);
                nodeStore.store(rootKey, root);
            } else {
                this.minData = root.getData().length;
            }
        }
        catch (final InterruptedException e)
        {
            throw new NodeStoreException(e);
        }
        catch (final ExecutionException e)
        {
            throw new NodeStoreException(e);
        }
        catch (final CancellationException e)
        {
            throw new NodeStoreException(e);
        }
        
        return root;
    }

    public void clear() { }
    public boolean containsKey(Object key) { return false; }
    public boolean containsValue(Object value) { return false; }
    public Set<Map.Entry<K,V>> entrySet(){ return null; }
    public boolean equals(Object o) { return false; }
    public V get(Object key){ return null; }
    public int hashCode(){ return 0; }
    public boolean isEmpty(){ return false; }
    public Set<K> keySet() { return null; }

    public V put(K key, V value){
    
        return null; 
    }
    
    public void putAll(Map<? extends K,? extends V> m) { }
    public V remove(Object key) { return null; }
    public int size() { return 0; }
    public Collection<V> values(){ return null; }
}
