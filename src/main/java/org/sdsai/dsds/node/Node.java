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
package org.sdsai.dsds.node;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import javax.xml.bind.annotation.XmlTransient;

/**
 * The basic object that is persisted to a large distributed 
 * storage via a {@link NodeStore}. Notice that a {@link Node}
 * does not posses any knowlede of the key it is stored under.
 * Only it's children or ancestor nodes.
 */
public class Node<USERKEY, STOREKEY> implements Serializable
{
    private List<USERKEY> data;
    private List<STOREKEY> ancestors;
    private List<STOREKEY> children;
    
    private int dataCap;
    private int ancestorsCap;
    private int childCap;

    /**
     * Create a Node that can store a single ancestor, child, and data object.
     */
    public Node() {
        this(1,1,1);
    }
    
    public Node(final int childCap) {
        this(childCap, 1, 1);
    }

    public Node(final int childCap,
                final int dataCap)
    {
        this(childCap, dataCap, 1);
    }
    
    public Node(final int childCap,
                final int dataCap,
                final int ancestorsCap)
    {
        this.childCap = childCap;
        this.dataCap = dataCap;
        this.ancestorsCap = ancestorsCap;
        
        this.data = new ArrayList<USERKEY>(dataCap);
        this.children = new ArrayList<STOREKEY>(childCap);
        this.ancestors = new ArrayList<STOREKEY>(ancestorsCap);
    }
    
    public List<USERKEY> getData() {
        return data;
    }
    
    public List<STOREKEY> getAncestors() {
        return ancestors;
    }
    
    public List<STOREKEY> getChildren() {
        return children;
    }
    
    public int getDataCap() {
        return dataCap;
    }
    
    public int getChildCap() {
        return childCap;
    }
    
    public int getAncestorsCap() {
        return ancestorsCap;
    }
    
    @JsonIgnore
    @XmlTransient
    public boolean isDataFull() {
        return dataCap == data.size();
    }
    
    @JsonIgnore
    @XmlTransient
    public boolean isChildrenFull() {
        return childCap == children.size();
    }
    
    @JsonIgnore
    @XmlTransient
    public boolean isAncestorsFull() {
        return ancestorsCap == ancestors.size();
    }
    
    public void setData(final List<USERKEY> data) {
        this.data = data;
    }
    
    public void setChildren(final List<STOREKEY> children) {
        this.children = children;
    }
    
    public void setAncestors(final List<STOREKEY> ancestors) {
        this.ancestors = ancestors;
    }
    
    public void setDataCap(final int dataCap) {
        this.dataCap = dataCap;
    }

    public void setChildCap(final int childCap) {
        this.childCap = childCap;
    }

    public void setAncestorCap(final int ancestorsCap) {
        this.ancestorsCap = ancestorsCap;
    }
    
    /**
     * Return true of this node has no data.
     *
     * @return true of this node has no data.
     */
    @JsonIgnore
    @XmlTransient
    public boolean isEmpty()
    {
        return data.isEmpty();
    }
    
    /**
     * Return true if this node has no children.
     *
     * @return true if this node has no children.
     */
    @JsonIgnore
    @XmlTransient
    public boolean isLeaf() {
        return children.isEmpty();
    }
    
    /**
     * Return true of this node has no ancestors.
     *
     * @return true if this node has no ancestors.
     */
    @JsonIgnore
    @XmlTransient
    public boolean isRoot() {
        return ancestors.isEmpty();
    }
    
    public String toString() {
        final StringBuilder sb = new StringBuilder("Node:\n");
        
        for( USERKEY k : data ) {
            sb.append("\tDATA: ").append(k).append("\n");
        }

        for( STOREKEY k : children ) {
            sb.append("\tCHILD: ").append(k).append("\n");
        }

        for( STOREKEY k : ancestors ) {
            sb.append("\tANCESTOR: ").append(k).append("\n");
        }
        
        
        return sb.toString();
    }
    
} // public class Node
