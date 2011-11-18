package org.sdsai.dsds;

public class NodeStoreException extends Exception
{
    public NodeStoreException() {
        super();
    }
    public NodeStoreException(final Throwable t) {
        super(t);
    }
    public NodeStoreException(final String msg) {
        super(msg);
    }
    public NodeStoreException(final String msg, final Throwable t) {
        super(msg, t);
    }
}
