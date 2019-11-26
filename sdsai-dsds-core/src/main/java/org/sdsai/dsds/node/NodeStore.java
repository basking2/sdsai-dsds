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

import java.util.concurrent.Future;

/**
 * <p>The bridge code from Java objects to stored documents.
 * Implementors of this interface will be doing much of what a DAO would do,
 * and a little more. Specifically functions like {@link #convert(Object)}
 * and {@link #generateKey(Node, Object)} are critical but not something a
 * traditional DAO would exposing.</p>
 *
 * <p>All exceptions are {@link NodeStoreException}s which is itself a
 * {@link RuntimeException}. Note that {@link #loadNode(Object)} is 
 * unique in that it will throw a {@link NodeStoreNodeNotFoundException}
 * (which is a type of {@link NodeStoreException}).
 *
 * @param <USERKEY> The user's key.
 * @param <STOREKEY> The key type used by the storage medium.
 * @param <VALUE> The values stored.
 */
public interface NodeStore<USERKEY, STOREKEY, VALUE>
{
    /**
     * Load the user's data.
     * @return the user's data or null if it is not found.
     * @throws NodeStoreException
     */
    VALUE loadData(STOREKEY key);

    /**
     * Load a node object from {@link NodeStore}.
     * @throws NodeStoreException
     * @throws NodeStoreNodeNotFoundException When a node is not found.
     */
    Node<USERKEY, STOREKEY> loadNode(STOREKEY key);
    
    /**
     * Store user data. If an object already exists at the key, it
     * should be replaced.
     * @throws NodeStoreException
     */
    void store(STOREKEY key, VALUE data);
    
    /**
     * Store the given node. If an object already exists at the key, it
     * should be replaced.
     * @throws NodeStoreException
     */
    void store(STOREKEY key, Node<USERKEY, STOREKEY> node);
    
    /**
     * Remove the given node key. If the key does not exist, this should
     * return without error.
     *
     * @throws NodeStoreException
     */
    void removeNode(STOREKEY key);

    /**
     * Remove the given data key. If the key does not exist, this should
     * return without error.
     *
     * @throws NodeStoreException
     */
    void removeData(STOREKEY key);

    /**
     * Generate a key to store a node or data element.
     * This key must be unique.
     *
     * @param node This is provided to implementations for some context.
     *             If generateKey is being called for the purposes of
     *             storing a user key, this will be null.
     * @param value If a key is being generated for a user key value, it will
     *              be passed along. This is most useful
     *              when things like a {@link org.sdsai.dsds.PagedList}
     *              are adding a value, but the List API does not 
     *              provide a way to specify a key.
     * @throws NodeStoreException
     */
    STOREKEY generateKey(Node<USERKEY, STOREKEY> node, VALUE value);
    
    /**
     * Convert user keys to store keys. This method, given a user's key,
     * must always produce the same store key.
     *
     * @throws NodeStoreException if any error occurs.
     */
    STOREKEY convert(USERKEY key);
    
} // public interface NodeStore
