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
package com.github.basking2.sdsai.dsds.mongo;

import com.github.basking2.sdsai.dsds.node.Node;
import com.github.basking2.sdsai.dsds.node.NodeStore;
import com.github.basking2.sdsai.dsds.node.NodeStoreException;
import com.github.basking2.sdsai.dsds.node.NodeStoreNodeNotFoundException;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import org.bson.types.ObjectId;

import static com.github.basking2.sdsai.dsds.mongo.MongoUtils.fromDBObject;
import static com.github.basking2.sdsai.dsds.mongo.MongoUtils.toDBObject;

public class MongoNodeStore<USERKEY, VALUE>
    implements NodeStore<USERKEY, String, VALUE>
{
    private DBCollection nodeCollection;
    private DBCollection dataCollection;
    private WriteConcern writeConcern;

    /**
     * @see #MongoNodeStore(DBCollection, DBCollection, WriteConcern)
     */
    public MongoNodeStore(final DBCollection nodeCollection,
                          final DBCollection dataCollection)
    {
        this(nodeCollection, dataCollection, WriteConcern.SAFE);
    }

    /**
     */
    public MongoNodeStore(final DBCollection nodeCollection,
                          final DBCollection dataCollection,
                          final WriteConcern writeConcern)
    {
        this.nodeCollection = nodeCollection;
        this.dataCollection = dataCollection;
        this.writeConcern = writeConcern;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public VALUE loadData(String key) {
        try
        {
            final DBObject dbo = dataCollection.findOne(
                new BasicDBObject("_id", key));
                
            if ( dbo == null )
                return null;

            @SuppressWarnings("unchecked")
            final VALUE v = (VALUE)fromDBObject(dbo);
            return v;
        }
        catch(MongoException e)
        {
            throw new NodeStoreException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Node<USERKEY, String> loadNode(String key) {
        try
        {
            final DBObject dbo = nodeCollection.findOne(
                new BasicDBObject("_id", key));
                
            if ( dbo == null ) {
                throw new NodeStoreNodeNotFoundException("key:"+key);
            }
            
            @SuppressWarnings("unchecked")
            final Node<USERKEY, String> n = (Node<USERKEY, String>) fromDBObject(dbo);
            return n;
        }
        catch(MongoException e)
        {
            throw new NodeStoreException(e);
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void store(String key, VALUE data) {
        try
        {
            final DBObject dbo = toDBObject(data);
            dbo.put("_id", key);
            dataCollection.save(dbo, writeConcern);
        }
        catch(MongoException e)
        {
            throw new NodeStoreException(e);
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void store(String key, Node<USERKEY, String> node) {
        try
        {
            final DBObject dbo = toDBObject(node);
            dbo.put("_id", key);
            nodeCollection.save(dbo, writeConcern);
        }
        catch(MongoException e)
        {
            throw new NodeStoreException(e);
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void removeNode(String key) {
        try
        {
            nodeCollection.remove(new BasicDBObject("_id", key), writeConcern);
        }
        catch(MongoException e)
        {
            throw new NodeStoreException(e);
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void removeData(String key) {
        try
        {
            dataCollection.remove(new BasicDBObject("_id", key), writeConcern);
        }
        catch(MongoException e)
        {
            throw new NodeStoreException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String generateKey(Node<USERKEY, String> node, VALUE value) {
        try 
        {
            return new ObjectId().toString();
        }
        catch(MongoException e)
        {
            throw new NodeStoreException(e);
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String convert(final USERKEY key) {
        return key.toString();
    }
}
