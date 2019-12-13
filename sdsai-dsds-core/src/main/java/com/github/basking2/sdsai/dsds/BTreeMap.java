package com.github.basking2.sdsai.dsds;

import java.util.Map;

public interface BTreeMap<K, V> extends Map<K, V> {

    /**
     * This function encodes and writes the key into the data structure.
     *
     * This is useful for situations where the user data is already in a data store and we are just building
     * additional indexes to it.
     *
     * @param key The key to add to the structure.
     * @return True if the key was inserted. False if the key already existed.
     */
    boolean putKey(final K key);
}
