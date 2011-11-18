package org.sdsai.dsds;

import org.sdsai.Key;

import java.util.concurrent.Future;

public interface NodeStore<K,V>
{
    /**
     */
    Future<V> loadData(K key) throws NodeStoreException;

    /**
     */
    Future<Node<K>> loadNode(K key) throws NodeStoreException;
    
    /**
     */
    Future<V> store(K key, V data) throws NodeStoreException;
    
    /**
     */
    Future<Node<K>> store(K key, Node<K> node) throws NodeStoreException;
    
    /**
     */
    Future<K> remove(K key) throws NodeStoreException;
    
    /**
     * Generate an ID to store a V value at. This is different than a
     * key a user will submit to a datastructure to store an object.
     * The key a user submits identifies the object's place in the
     * datastructure whereas the key this will generate is used
     * as a key into the storage layer.
     */
    Future<K> generateKey() throws NodeStoreException;

} // public interface NodeStore
