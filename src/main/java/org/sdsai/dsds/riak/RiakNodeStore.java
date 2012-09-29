package org.sdsai.dsds.riak;

import org.sdsai.dsds.node.NodeStore;
import org.sdsai.dsds.node.Node;
import org.sdsai.dsds.node.NodeStoreException;
import org.sdsai.dsds.node.NodeStoreNodeNotFoundException;

import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.cap.VClock;
import com.basho.riak.client.cap.BasicVClock;
import com.basho.riak.client.http.response.FetchResponse;
import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.operations.FetchObject;
import com.basho.riak.client.RiakRetryFailedException;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static java.util.UUID.randomUUID;

/**
 * A simple RiakNodeStore in which the user must provide the serialization classes.
 * @see com.basho.riak.client.convert.JSONConverter
 */
public class RiakNodeStore<USERKEY, VALUE> implements NodeStore<USERKEY, String, VALUE>
{
    private Logger logger = LoggerFactory.getLogger(RiakNodeStore.class);
    private IRiakClient riakClient;
    private String dataBucket;
    private String nodeBucket;
    private ObjectMapper objectMapper;

    private Class<VALUE> valueClass;

    /**
     * Create a new RiakNodeStore that manipulates objects in a particular bucket.
     * @param riakClient The RiakClient to use.
     * @param nodeBucket The bucket that nodes will be stored in.
     * @param dataBucket The bucket that user data will be stored in.
     * @param valueClass The value class to convert members back from.
     */
    public RiakNodeStore(final IRiakClient riakClient,
                         final String nodeBucket,
                         final String dataBucket,
                         final Class<VALUE> valueClass)
    {
        this.riakClient = riakClient;
        this.dataBucket = dataBucket;
        this.nodeBucket = nodeBucket;
        this.objectMapper = new ObjectMapper();
        this.valueClass = valueClass;
    }

    /**
     * This is the same as {@link #RiakNodeStore(IRiakClient, String, String, Class<VALUE>)}
     * but will all objects are in 1 bucket.
     *
     * @param riakClient The IRiakClient.
     * @param bucket The bucket that nodes and user data will be stored in.
     * @param valueClass The value class to convert members back from.
     */
    public RiakNodeStore(final IRiakClient riakClient,
                         final String bucket,
                         final Class<VALUE> valueClass)
    {
        this(riakClient, bucket, bucket, valueClass);
    }

    /**
     * Calls {@link Object#toString()} on key.
     */
    @Override
    public String convert(final USERKEY key)
    {
        return key.toString();
    }

    /**
     * Return a string representation of a call to {@link java.util.UUID#randomUUID()}.
     *
     * @param node Unnecessary node. May be null.
     *
     * @return the string representation of a call to {@link java.util.UUID#randomUUID()}.
     */
    @Override
    public String generateKey(final Node<USERKEY, String> node)
    {
        return randomUUID().toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public VALUE loadData(final String key) throws NodeStoreException
    {
        try 
        {
            logger.debug("Loading data@"+key);

	    final IRiakObject riakObject = getDataBucket().fetch(key).execute();

	    if ( riakObject == null ) {
                logger.debug("Not found: data@"+key);
		return null;
	    }

            logger.debug("Found: data@"+key);
            return objectMapper.readValue(riakObject.getValueAsString(), valueClass);
        }
        catch (final IOException e)
        {
            throw new NodeStoreException(e);
        }
        catch (final RiakRetryFailedException e)
        {
            throw new NodeStoreException(e);
        }
    }

    /**
     * Get the Node Bucket from Riak.
     */
    private Bucket getDataBucket() throws RiakRetryFailedException
    {
	final Bucket bucket = riakClient.fetchBucket(dataBucket).execute();

        if ( bucket == null )
        {
            throw new NodeStoreException("Bucket "+dataBucket+" not found.");
        }

        return bucket;
    }

    /**
     * Get the Node Bucket from Riak.
     */
    private Bucket getNodeBucket() throws RiakRetryFailedException
    {
	final Bucket bucket = riakClient.fetchBucket(nodeBucket).execute();

        if ( bucket == null )
        {
            throw new NodeStoreException("Bucket "+nodeBucket+" not found.");
        }

	return bucket;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Node<USERKEY, String> loadNode(final String key) 
	throws NodeStoreNodeNotFoundException
    {
        try 
        {
	    final IRiakObject riakObject = getNodeBucket().fetch(key).execute();

            logger.debug("Loading node@"+key);
	    if ( riakObject == null ) {
                logger.debug("Not found: node@"+key);
		throw new NodeStoreNodeNotFoundException("Could not find the node with key "+key);
	    }

            logger.debug("Not found: node@"+key);
            return objectMapper.readValue(riakObject.getValueAsString(), Node.class);
        }
        catch (final IOException e)
        {
            throw new NodeStoreException(e);
        }
        catch (final RiakRetryFailedException e)
        {
            throw new NodeStoreException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeData(final String key)
    {
        try 
        {
            getDataBucket().delete(key).execute();
        }
        catch (final RiakException e)
        {
            throw new NodeStoreException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeNode(final String key)
    {
        try 
        {
            getNodeBucket().delete(key).execute();
        }
        catch (final RiakException e)
        {
            throw new NodeStoreException(e);
        }
    }

    private VClock randomVClock()
    {
        final long t = System.currentTimeMillis();
        byte[] b = new byte[8];

        // Most significant byte goes at index 0.
        b[0] = (byte)((t & 0xff00000000000000L) >>> 56);
        b[1] = (byte)((t & 0x00ff000000000000L) >>> 48);
        b[2] = (byte)((t & 0x0000ff0000000000L) >>> 40);
        b[3] = (byte)((t & 0x000000ff00000000L) >>> 32);
        b[4] = (byte)((t & 0x00000000ff000000L) >>> 24);
        b[5] = (byte)((t & 0x0000000000ff0000L) >>> 16);
        b[6] = (byte)((t & 0x000000000000ff00L) >>>  8);
        b[7] = (byte)((t & 0x00000000000000ffL)       );

        return new BasicVClock(b);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void store(final String key, final Node<USERKEY, String> node)
    {
        try 
        {
            getNodeBucket().store(key, objectMapper.writeValueAsString(node)).execute();
        }
        catch (final IOException e)
        {
            throw new NodeStoreException(e);
        }
        catch (final RiakRetryFailedException e)
        {
            throw new NodeStoreException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void store(final String key, final VALUE data)
    {
        try 
        {
            getDataBucket().store(key, objectMapper.writeValueAsString(data)).execute();
        }
        catch (final IOException e)
        {
            throw new NodeStoreException(e);
        }
        catch (final RiakRetryFailedException e)
        {
            throw new NodeStoreException(e);
        }
    }
}
