SDSAI-DSDS
==========

ABOUT
-----

Sdsai-dsds is a Document Store Data Structure Library distributed under the 
MIT license. 

A strong emphasis is placed on minimizing reads and writes to the data store
and implementing the Java Collections API where reasonable. Iterators
are also an important component of reducing the amount of code required
to interact with this data structure when the user wants their data.

MOTIVATION
----------

When working with document store databases such as Cassandra, picking
an evenly distributed key has very desirable properties in that it
distributes data evenly across the cluster nodes. This, however, means that
iterating over the data or searching the data is much more difficult.

This project implements common data structures using a basic storage API.
This allows those data structures to perform their operations on a document 
storage system while allowing the user to store their data using evenly 
distributed key values.

EXTENDABILITY
-------------

NodeStore

The interface  org.sdsai.dsds.node.NodeStore is all that must be implemented
for the data structures to be used with a new storage system.

Data Structures

The NodeStore interface stores a Node class which is a somewhat abstract
representation of a directed graph node. This allows for future implementation
of new data structures or algorithms (hopefully) without changes or deep 
consideration being given to the persistence layer.

IMPLEMENTED DATA STRUCTURES
---------------------------

 o B-Tree
 o Paged Linked List

IMPLEMENTED DATABASES
---------------------

* DirectoryNodeStore - Stores data structures in a file system. One file
                       per Node and one file per user data object.
* MongoDB - Implemented using the raw MongoDB driver.
* Riak - Implemented using Jackson 2 to serialize Node and user data.

