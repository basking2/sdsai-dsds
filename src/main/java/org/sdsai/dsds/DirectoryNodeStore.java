package org.sdsai.dsds;

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

/**
 * A trivial storage class that puts all nodes into a single directory
 * with filenames represented by the hex-encoding of their key names.
 */
public class DirectoryNodeStore<K extends Serializable, D extends Serializable> 
implements NodeStore<K, D>
{

    private File directory;
    
    public DirectoryNodeStore(final File directory) {
        this.directory = directory;
        
        if ( ! directory.exists() )
            directory.mkdirs();
    }
    
    @Override
    public Future<K> remove(final K key) {
        final FutureTask<K> f = new FutureTask<K>( new Callable<K>() {
            public K call()
            throws FileNotFoundException,
                   IOException,
                   ClassNotFoundException
            {
                final File file = keyToFile(key);
                
                file.delete();
                
                return key;
            }
        });
        
        f.run();
        
        return f;
    }

    @Override
    public Future<Node<K>> loadNode(final K key) {
        return loadHelper(key, (Node<K>) null);
    }
    
    @Override
    public Future<D> loadData(final K key){ 
        return loadHelper(key, (D) null);
    }
    
    @Override
    public Future<Node<K>> store(final K key, final Node<K> node) {
        return storeHelper(key, node);
    }
    
    @Override
    public Future<D> store(final K key, final D data){ 
        return storeHelper(key, data);
    }
    
    @Override
    public Future<K> generateKey()
    {
        throw new RuntimeException("IMPLEMENT ME");
    }
    
    private <D2> Future<D2> loadHelper(final K key, final D2 type)
    {
        final FutureTask<D2> f = new FutureTask<D2>( new Callable<D2>() {
            public D2 call()
            throws FileNotFoundException,
                   IOException,
                   ClassNotFoundException
            {
                final File file = keyToFile(key);
                final FileInputStream fis = new FileInputStream(keyToFile(key));
                final ObjectInputStream ois = new ObjectInputStream(fis);
                final D2 n = (D2) ois.readObject();
                
                ois.close();
                fis.close();
                
                return n;
            }
        });
        
        f.run();
        
        return f;
    }
    
    private <D2> Future<D2> storeHelper(final K key, final D2 d)
    {
        final FutureTask<D2> f = new FutureTask<D2>( new Callable<D2>() {
            @Override
            public D2 call() throws IOException
            {
                final FileOutputStream fos =
                    new FileOutputStream(keyToFile(key));
                final ObjectOutputStream oos =
                    new ObjectOutputStream(fos);
                
                oos.writeObject(d);
                
                oos.close();
                fos.close();
                
                return d;
            }
        });
        
        f.run();
        
        return f;
    }
    
    private File keyToFile(final K key) {
        return new File(directory, key.toString());
    }
} 
