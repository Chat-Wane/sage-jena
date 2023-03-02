package fr.gdd.sage.arq;

import java.util.Iterator;

import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.tdb2.store.NodeId;
import org.apache.jena.tdb2.store.nodetable.NodeTable;

import fr.gdd.sage.generics.Pair;
import fr.gdd.sage.interfaces.BackendIterator;
import fr.gdd.sage.interfaces.SPOC;
import fr.gdd.sage.io.SageOutput;
import fr.gdd.sage.jena.SerializableRecord;

import org.apache.jena.dboe.trans.bplustree.JenaIterator;



/**
 * Volcano iterator that wraps a backend iterator meant for compiled
 * query execution. Among others, `next()` returns {@link Quad} which
 * contains four {@link Node}; and the `hasNext()` checks if it
 * reached the timeout before saving into the shared
 * {@link SageOutput}.
 */
public class VolcanoIterator implements Iterator<Quad> {

    public BackendIterator<NodeId, SerializableRecord> wrapped;
    NodeTable nodeTable;
    long deadline;

    SageOutput<?> output;
    Integer id;

    // Cannot pause at first execution of the `hasNext()`.
    boolean first = false;


    
    public VolcanoIterator (BackendIterator<NodeId, SerializableRecord> iterator, NodeTable nodeTable,
                            long deadline, SageOutput<?> output, Integer id) {
        this.wrapped = iterator;
        this.nodeTable = nodeTable;
        this.deadline = deadline;
        this.output = output;
        this.id = id;
    }
    
    @Override
    public boolean hasNext() {
        if (!first && System.currentTimeMillis() > deadline) {
            if (this.output.getState() == null) {
                var toSave = new Pair(id, this.wrapped.current());
                this.output.addState(toSave);
            } else {
                var toSave = new Pair(id, this.wrapped.previous());
                this.output.addState(toSave);
            }
            
            return false;
        }
        first = false;
        
        return wrapped.hasNext();
    }

    @Override
    public Quad next() {
        wrapped.next();
        Node gx = Quad.defaultGraphIRI;
        Node sx = nodeTable.getNodeForNodeId(wrapped.getId(SPOC.SUBJECT));
        Node px = nodeTable.getNodeForNodeId(wrapped.getId(SPOC.PREDICATE));
        Node ox = nodeTable.getNodeForNodeId(wrapped.getId(SPOC.OBJECT));

        // (TODO) ugly fix. How to make a better handling of scans?
        JenaIterator it = (JenaIterator) wrapped;
        if (it.goRandom) {
            if (id == 0) {
                it.finished = false;
            } else {
                it.finished = true;
            }
        }
        
        return Quad.create(gx, sx, px, ox);
    }

    public void skip(SerializableRecord to) {
        first = true;
        wrapped.skip(to);
    }
}
