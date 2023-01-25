package fr.gdd.volcano;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.dboe.base.record.Record;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.tdb2.store.NodeId;
import org.apache.jena.tdb2.store.nodetable.NodeTable;

import fr.gdd.common.BackendIterator;
import fr.gdd.common.SPOC;
import fr.gdd.common.SageOutput;



public class VolcanoIterator implements Iterator<Quad> {

    public BackendIterator<NodeId, Record> wrapped;
    NodeTable nodeTable;
    long deadline;

    SageOutput output;
    Integer id;
    Map<Integer, VolcanoIterator> iterators_map;
    
    boolean first = false;


    
    public VolcanoIterator (BackendIterator<NodeId, Record> iterator, NodeTable nodeTable, long deadline,
                            Map<Integer, VolcanoIterator> iterators, SageOutput output, Integer id) {
        this.wrapped = iterator;
        this.nodeTable = nodeTable;
        this.deadline = deadline;
        this.output = output;
        this.iterators_map = iterators;
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
            
            // System.out.printf("VolcanoIterator n°%s deadline, nb iterators %s\n", id, iterators_map.size());
            // for (Entry<Integer, VolcanoIterator> e : this.iterators_map.entrySet()) {
            //     if (e.getKey() < id) {
            //         var toSave = new Pair(e.getKey(), e.getValue().wrapped.previous());
            //         this.output.addState(toSave);
            //     }
            // }
            
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
        return Quad.create(gx, sx, px, ox);
    }

    public void skip(Record to) {
        first = true;
        wrapped.skip(to);
    }

}
