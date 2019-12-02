package com.github.basking2.sdsai.dsds.node.tx;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;

import java.util.List;
import java.util.Map;

import com.github.basking2.sdsai.dsds.BTree;
import com.github.basking2.sdsai.dsds.PagedList;
import com.github.basking2.sdsai.dsds.node.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Build transactional datastructures using {@link Proxy}.
 *
 * Any operation on {@link BTree} or {@link PagedList} or similar may result in several writes to the {@link NodeStore}.
 * The {@link TransactionalNodeStore} collects those operations and executes them at one time. It also skips operations
 * that are obviated by later operations. For example, writes are skipped if a delete happens later in the history.
 * However, {@link TransactionalNodeStore#commit()} must be called manually.
 *
 * This set of factories proxy calls to data structures so that they may be wrapped with calls to {@link TransactionalNodeStore#commit()}}.
 *
 */
public class DataStructureFactory
{
    private static final Logger LOG  = LoggerFactory.getLogger(DataStructureFactory.class);

    private DataStructureFactory()
    {
    }

    /**
     * Build a new DataStructureFactory.
     *
     * @throws RuntimeException If a TransactionalNodeStore is passed as the nodeStore parameter.
     */
    private static <USERKEY, STOREKEY, VALUE> TransactionalNodeStore<USERKEY, STOREKEY, VALUE> txNodeStore(
        final NodeStore<USERKEY, STOREKEY, VALUE> nodeStore)
    {
        if ( nodeStore instanceof TransactionalNodeStore )
        {
            LOG.warn("Use of a TransactionalNodeStore as the input to DataStructureFactory may result in data never being comitted.");
        }

        return new TransactionalNodeStore<>(nodeStore);
    }

    private static <USERKEY, STOREKEY, VALUE> InvocationHandler buildTxCommittingInvocationHandler(
        final NodeStore<USERKEY, STOREKEY, VALUE> nodeStore,
        final Object dispatchObject)
    {

        final TransactionalNodeStore<USERKEY, STOREKEY, VALUE> transactionalNodeStore = txNodeStore(nodeStore);
        final InvocationHandler ivh = new InvocationHandler()
        {
            @Override
            public Object invoke(final Object proxy, final Method method, final Object[] args)
            {
                try
                {
                    final Object returnObject = method.invoke(dispatchObject, args);

                    transactionalNodeStore.commit();

                    return returnObject;
                }
                catch (final InvocationTargetException ite)
                {
                    throw new RuntimeException("Illegal access to method.", ite);
                }
                catch (final IllegalAccessException iae)
                {
                    throw new RuntimeException("Illegal access to method.", iae);
                }
            }
        };

        return ivh;
    }

    public static <STOREKEY, VALUE> List<VALUE> pagedList(
        final STOREKEY root,
        final NodeStore<STOREKEY, STOREKEY, VALUE> nodeStore,
        final int size)
    {
        final PagedList<STOREKEY, VALUE> pagedList = new PagedList<>(root, nodeStore, size);

        @SuppressWarnings("unchecked")
        final List<VALUE> list = (List<VALUE>)
            Proxy.newProxyInstance(
                PagedList.class.getClassLoader(),
                new Class<?>[]{ List.class },
                buildTxCommittingInvocationHandler(nodeStore, pagedList));

        return list;
    }

    public static <STOREKEY, VALUE> List<VALUE> pagedList(
        final STOREKEY root,
        final NodeStore<STOREKEY, STOREKEY, VALUE> nodeStore)
    {
        final PagedList<STOREKEY, VALUE> pagedList = new PagedList<>(root, nodeStore);

        @SuppressWarnings("unchecked")
        final List<VALUE> list = (List<VALUE>)
            Proxy.newProxyInstance(
                PagedList.class.getClassLoader(),
                new Class<?>[]{ List.class },
                buildTxCommittingInvocationHandler(nodeStore, pagedList));

        return list;
    }

    public static <USERKEY, STOREKEY, VALUE> Map<USERKEY, VALUE> bTree(
        final USERKEY root,
        final NodeStore<USERKEY, STOREKEY, VALUE> nodeStore,
        final int size)
    {
        final BTree<USERKEY, STOREKEY, VALUE> bTree = new BTree<>(root, nodeStore, size);

        @SuppressWarnings("unchecked")
        final Map<USERKEY, VALUE> map = (Map<USERKEY, VALUE>)
            Proxy.newProxyInstance(
                BTree.class.getClassLoader(),
                new Class<?>[]{ Map.class },
                buildTxCommittingInvocationHandler(nodeStore, bTree));

        return map;
    }

    public static <USERKEY, STOREKEY, VALUE> Map<USERKEY, VALUE> bTree(
        final USERKEY root,
        final NodeStore<USERKEY, STOREKEY, VALUE> nodeStore)
    {
        final BTree<USERKEY, STOREKEY, VALUE> bTree = new BTree<>(root, nodeStore);

        @SuppressWarnings("unchecked")
        final Map<USERKEY, VALUE> map = (Map<USERKEY, VALUE>)
            Proxy.newProxyInstance(
                BTree.class.getClassLoader(),
                new Class<?>[]{ Map.class },
                buildTxCommittingInvocationHandler(nodeStore, bTree));

        return map;
    }
}
