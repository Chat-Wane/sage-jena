package fr.gdd.sage.arq;

import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.tdb2.store.DatasetGraphTDB;
import org.apache.jena.tdb2.store.NodeId;
import org.apache.jena.tdb2.store.nodetable.NodeTable;

import fr.gdd.sage.interfaces.BackendIterator;
import fr.gdd.sage.interfaces.RandomIterator;
import fr.gdd.sage.io.SageInput;
import fr.gdd.sage.io.SageOutput;
import fr.gdd.sage.jena.PreemptableTupleTable;
import fr.gdd.sage.jena.SerializableRecord;

import org.apache.jena.sparql.engine.ExecutionContext;



/**
 * A volcano iterator factory to ease creation of iterators, one per
 * execution context.
 **/
public class VolcanoIteratorFactory {
    
    SageInput<?> input;
    SageOutput<?> output;
    long deadline;

    // do scans provide random bindings in their respective allowed
    // range ?
    boolean shouldRandom = false;

    private NodeTable quadNodeTable;
    private NodeTable tripleNodeTable;
    private PreemptableTupleTable preemptableQuadTupleTable;
    private PreemptableTupleTable preemptableTripleTupleTable;
    

    
    public VolcanoIteratorFactory(ExecutionContext context) {
        this.output = context.getContext().get(SageConstants.output);
        this.input  = context.getContext().get(SageConstants.input);
        // (TODO) change this 
        this.deadline = System.currentTimeMillis() + input.getTimeout();
        this.deadline = System.currentTimeMillis() + 1000;

        var graph = (DatasetGraphTDB) context.getDataset();
        var nodeQuadTupleTable = graph.getQuadTable().getNodeTupleTable();
        var nodeTripleTupleTable = graph.getTripleTable().getNodeTupleTable();
        quadNodeTable = graph.getQuadTable().getNodeTupleTable().getNodeTable();
        tripleNodeTable = graph.getTripleTable().getNodeTupleTable().getNodeTable();
        preemptableTripleTupleTable = new PreemptableTupleTable(nodeTripleTupleTable.getTupleTable());
        preemptableQuadTupleTable   = new PreemptableTupleTable(nodeQuadTupleTable.getTupleTable());

    }

    public VolcanoIteratorFactory provideRandomScans() {
        this.shouldRandom = true;
        return this;
    }

    public VolcanoIteratorFactory provideRegularScans() {
        this.shouldRandom = false;
        return this;
    }
    
    public VolcanoIterator getScan(Tuple<NodeId> pattern, Integer id) {
        BackendIterator<NodeId, SerializableRecord> wrapped = null;
        if (pattern.len() < 4) {
            wrapped = preemptableTripleTupleTable.preemptable_find(pattern);
        } else {
            wrapped = preemptableQuadTupleTable.preemptable_find(pattern);
        }
        
        if (shouldRandom) {
            ((RandomIterator) wrapped).random();
        }
        
        if (pattern.len() < 4) {
            return new VolcanoIterator(wrapped, tripleNodeTable, deadline, output, id);
        } else {
            return new VolcanoIterator(wrapped, quadNodeTable, deadline, output, id);
        }
    }
    
}
