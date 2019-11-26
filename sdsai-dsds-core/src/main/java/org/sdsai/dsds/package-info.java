/**
 * <p>
 * The DSDS package is the Document Store Data Structure package.
 * It contains datastructure implementations meant to be stored
 * in large, distributed, and potentially slower accessing
 * datastores such as Cassandra. By "slow" we mean relative to memory or 
 * local disk accesses.
 * </p>
 * <p>
 * All datastructures contained here typically will not protect against
 * concurrent modification. If you have a datastore that supports
 * MVCC, that logic should be built into the 
 * {@link org.sdsai.dsds.node.NodeStore}
 * that backs your data objects. Trasactionality should also be put
 * in the NodeStore. 
 * </p>
 * <p>
 * All datastructures take pain to never delete their
 * root {@link org.sdsai.dsds.node.Node} as it is expected that 
 * these datastructures will have well known names on their servers.
 * </p>
 */
package org.sdsai.dsds;
