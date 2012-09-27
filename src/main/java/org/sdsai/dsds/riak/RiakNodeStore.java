package org.sdsai.dsds.riak;

import org.sdsai.dsds.node.NodeStore;
import org.sdsai.dsds.node.Node;
import org.sdsai.dsds.node.NodeStoreException;

import com.basho.riak.client.cap.VClock;
import com.basho.riak.client.cap.BasicVClock;
import com.basho.riak.client.convert.Converter;
import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.http.response.FetchResponse;
import com.basho.riak.client.RiakRetryFailedException;

import static java.util.UUID.randomUUID;

/**
 * A simple RiakNodeStore in which the user must provide the serialization classes.
 * @see com.basho.riak.client.convert.JSONConverter
 */
public class RiakNodeStore<USERKEY, VALUE> implements NodeStore<USERKEY, String, VALUE>
{

    private IRiakClient riakClient;
    private String dataBucket;
    private String nodeBucket;

    private Converter<VALUE> dataConverter;
    private Converter<Node<USERKEY, String>> nodeConverter;

    /**
     * Create a new RiakNodeStore that manipulates objects in a particular bucket.
     * @param riakClient The RiakClient to use.
     * @param nodeBucket The bucket that nodes will be stored in.
     * @param dataBucket The bucket that user data will be stored in.
     * @param nodeConverter Bridges java objects to riak storage representations.
     * @param dataConverter Bridges java objects to riak storage representations.
     */
    public RiakNodeStore(final IRiakClient riakClient,
                         final String nodeBucket,
                         final String dataBucket,
                         final Converter<Node<USERKEY, String>> nodeConverter,
                         final Converter<VALUE> dataConverter)
    {
        this.riakClient = riakClient;
        this.dataBucket = dataBucket;
        this.nodeBucket = nodeBucket;
        this.nodeConverter = nodeConverter;
        this.dataConverter = dataConverter;
    }

    /**
     * This is the same as {@link #RiakNodeStore(IRiakClient, String, String, Converter, Converter)} but will all objects are in 1 bucket.
     *
     * @param riakClient The IRiakClient.
     * @param bucket The bucket that nodes and user data will be stored in.
     * @param nodeConverter Bridges java objects to riak storage representations.
     * @param dataConverter Bridges java objects to riak storage representations.
     */
    public RiakNodeStore(final IRiakClient riakClient,
                         final String bucket,
                         final Converter<Node<USERKEY, String>> nodeConverter,
                         final Converter<VALUE> dataConverter)
    {
        this(riakClient, bucket, bucket, nodeConverter, dataConverter);
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
            return dataConverter.toDomain(riakClient.fetchBucket(dataBucket).execute().fetch(key).execute());
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
    public Node<USERKEY, String> loadNode(final String key)
    {
        try 
        {
            return nodeConverter.toDomain(riakClient.fetchBucket(nodeBucket).execute().fetch(key).execute());
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
            riakClient.fetchBucket(dataBucket).execute().delete(key);
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
    public void removeNode(final String key)
    {
        try 
        {
            riakClient.fetchBucket(nodeBucket).execute().delete(key);
        }
        catch (final RiakRetryFailedException e)
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
            riakClient.fetchBucket(nodeBucket).execute().store(key, nodeConverter.fromDomain(node, randomVClock()));
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
            riakClient.fetchBucket(dataBucket).execute().store(key, dataConverter.fromDomain(data, randomVClock()));
        }
        catch (final RiakRetryFailedException e)
        {
            throw new NodeStoreException(e);
        }
    }
}
