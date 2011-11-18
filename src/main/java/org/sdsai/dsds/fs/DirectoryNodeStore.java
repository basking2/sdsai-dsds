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
package org.sdsai.dsds.fs;

import java.io.File;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.io.Serializable;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.sdsai.dsds.node.Node;
import org.sdsai.dsds.node.NodeStore;
import org.sdsai.dsds.node.NodeStoreException;
import org.sdsai.dsds.node.NodeStoreNodeNotFoundException;

/**
 * A trivial storage class that puts all nodes into a single directory
 * with filenames represented by the hex-encoding of their key names.
 */
public class DirectoryNodeStore<K extends Serializable, D extends Serializable> 
implements NodeStore<K, File, D>
{

    private File directory;
    private long idGenerator;
    
    public DirectoryNodeStore(final String directory) {
        this(new File(directory));
    }
    
    public DirectoryNodeStore(final File directory) {
        this.directory = directory;
        
        if ( ! directory.exists() )
            directory.mkdirs();
            
        idGenerator = 0L;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void removeNode(final File key) throws NodeStoreException {
        key.delete();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeData(final File key) throws NodeStoreException {
        key.delete();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Node<K,File> loadNode(final File key) throws NodeStoreException  {
        final Node<K,File> n = loadHelper(key, (Node<K,File>) null);
        
        if ( n == null )
            throw new NodeStoreNodeNotFoundException("Could not find node "+key);

        return n;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public D loadData(final File key) throws NodeStoreException { 
        return loadHelper(key, (D) null);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void store(final File key, final Node<K,File> node) 
    throws NodeStoreException
    {
        storeHelper(key, node);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void store(final File key, final D data) throws NodeStoreException { 
        storeHelper(key, data);
    }
    
    private <D2> D2 loadHelper(final File file, final D2 type) 
    throws NodeStoreException 
    {
        try {
            final FileInputStream fis = new FileInputStream(file);
            final ObjectInputStream ois = new ObjectInputStream(fis);
            final D2 n = (D2) ois.readObject();

            ois.close();
            fis.close();

            return n;
        } catch (final FileNotFoundException e) {
            return null;
        } catch (final IOException e) {
            throw new NodeStoreException(e);
        } catch (final ClassNotFoundException e) {
            throw new NodeStoreException(e);
        }
    }
    
    private <D2> void storeHelper(final File file, final D2 d) 
    throws NodeStoreException 
    {
        try {
            final FileOutputStream fos = new FileOutputStream(file);
            final ObjectOutputStream oos = new ObjectOutputStream(fos);
                
            oos.writeObject(d);
                
            oos.close();
            fos.close();
        } catch (final IOException e) {
            throw new NodeStoreException(e);
        }            
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public File generateKey(final Node<K,File> node)
    {
        File f;
        
        do {
            f = new File( directory, java.util.UUID.randomUUID().toString());
        } while ( f.exists());
        
        return f;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public File convert(K key) throws NodeStoreException
    {
        return new File(directory, key.toString());
    }
} 
