package fr.gdd.sage.arq;

import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.tdb2.store.DatasetGraphTDB;
import org.apache.jena.tdb2.store.NodeId;

import fr.gdd.sage.interfaces.BackendIterator;
import fr.gdd.sage.interfaces.RandomIterator;
import fr.gdd.sage.interfaces.SageInput;
import fr.gdd.sage.interfaces.SageOutput;
import fr.gdd.sage.jena.JenaBackend;
import fr.gdd.sage.jena.PreemptableTupleTable;

import org.apache.jena.dboe.base.record.Record;
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
    boolean shouldRandom = true;
    
    private PreemptableTupleTable preemptableQuadTupleTable;
    private PreemptableTupleTable preemptableTripleTupleTable;
    

    
    public VolcanoIteratorFactory(SageInput<?> input, SageOutput<?> output, ExecutionContext context) {
        this.input = input;
        this.output = output;
        this.deadline = System.currentTimeMillis() + input.getTimeout();

        var graph = (DatasetGraphTDB) context.getDataset();
        var nodeQuadTupleTable = graph.getQuadTable().getNodeTupleTable();
        var nodeTripleTupleTable = graph.getTripleTable().getNodeTupleTable();
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
        BackendIterator<NodeId, Record> wrapped = null;
        JenaBackend backend = (JenaBackend) input.getBackend();
        // if (pattern.len() < 4) {
        wrapped = // backend.search(pattern.get(0), pattern.get(1), pattern.get(2));
            preemptableTripleTupleTable.preemptable_find(pattern);
        //} else {
        //            wrapped = backend.search(pattern.get(1), pattern.get(2), pattern.get(3), pattern.get(0));
        // }

        if (shouldRandom) {
            ((RandomIterator) wrapped).random();
        }
        
        return new VolcanoIterator(wrapped, backend.getNodeTable(), deadline, output, id);
    }

}
