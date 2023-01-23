package fr.gdd.jena;

import java.util.Iterator;

import org.apache.jena.dboe.base.record.Record;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.tdb2.store.NodeId;
import org.apache.jena.tdb2.store.nodetable.NodeTable;

import fr.gdd.common.BackendIterator;
import fr.gdd.common.SPOC;



public class VolcanoIterator implements Iterator<Quad> {

    BackendIterator<NodeId, Record> wrapped;
    NodeTable nodeTable;
    
    public VolcanoIterator (BackendIterator<NodeId, Record> iterator, NodeTable nodeTable) {
        this.wrapped = iterator;
        this.nodeTable = nodeTable;
    }
    
    @Override
    public boolean hasNext() {
        return wrapped.hasNext();
    }

    @Override
    public Quad next() {
        wrapped.next();
        Node gx = Quad.defaultGraphIRI;
        Node sx = nodeTable.getNodeForNodeId(wrapped.getId(SPOC.SUBJECT));
        Node px = nodeTable.getNodeForNodeId(wrapped.getId(SPOC.PREDICATE));
        Node ox = nodeTable.getNodeForNodeId(wrapped.getId(SPOC.OBJECT));
        return Quad.create(gx, sx, px, ox);
    }

}
