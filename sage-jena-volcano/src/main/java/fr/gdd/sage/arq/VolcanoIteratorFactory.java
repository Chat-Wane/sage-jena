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

import java.util.Map;


/**
 * A volcano iterator factory to ease creation of iterators, one per
 * execution context.
 **/
public class VolcanoIteratorFactory {
    
    SageInput<?> input;
    SageOutput<?> output;
    long deadline;
    Map<Integer, VolcanoIterator> iterators;

    private NodeTable quadNodeTable;
    private NodeTable tripleNodeTable;
    private PreemptableTupleTable preemptableQuadTupleTable;
    private PreemptableTupleTable preemptableTripleTupleTable;
    

    
    public VolcanoIteratorFactory(ExecutionContext context) {
        this.output = context.getContext().get(SageConstants.output);
        this.input  = context.getContext().get(SageConstants.input);

        this.deadline = this.input.getDeadline();
        this.iterators = context.getContext().get(SageConstants.iterators);
        
        var graph = (DatasetGraphTDB) context.getDataset();
        var nodeQuadTupleTable = graph.getQuadTable().getNodeTupleTable();
        var nodeTripleTupleTable = graph.getTripleTable().getNodeTupleTable();
        quadNodeTable = graph.getQuadTable().getNodeTupleTable().getNodeTable();
        tripleNodeTable = graph.getTripleTable().getNodeTupleTable().getNodeTable();
        preemptableTripleTupleTable = new PreemptableTupleTable(nodeTripleTupleTable.getTupleTable());
        preemptableQuadTupleTable   = new PreemptableTupleTable(nodeQuadTupleTable.getTupleTable());

    }
    
    public VolcanoIterator getScan(Tuple<NodeId> pattern, Integer id) {
        BackendIterator<NodeId, SerializableRecord> wrapped = null;
        if (pattern.len() < 4) {
            wrapped = preemptableTripleTupleTable.preemptable_find(pattern);
        } else {
            wrapped = preemptableQuadTupleTable.preemptable_find(pattern);
        }

        if (input.isRandomWalking()) {
            ((RandomIterator) wrapped).random();
        }

        VolcanoIterator volcanoIterator = (pattern.len() < 4) ?
                new VolcanoIterator(wrapped, tripleNodeTable, input, output, id):
                new VolcanoIterator(wrapped, quadNodeTable, input, output, id);

        // Check if it is a preemptive iterator that should jump directly to its resume state.
        if (!iterators.containsKey(id) && !input.isRandomWalking()) {
            if (input != null && input.getState() != null && input.getState().containsKey(id)) {
                volcanoIterator.skip((SerializableRecord) input.getState(id));
            }
        }
        iterators.put(id, volcanoIterator); // register and/or erase previous iterator

        return volcanoIterator;
    }

    public VolcanoIteratorTupleId getScanOnTupleId(Tuple<NodeId> pattern, Integer id) {
        BackendIterator<NodeId, SerializableRecord> wrapped = null;
        if (pattern.len() < 4) {
            wrapped = preemptableTripleTupleTable.preemptable_find(pattern);
        } else {
            wrapped = preemptableQuadTupleTable.preemptable_find(pattern);
        }

        if (input.isRandomWalking()) {
            ((RandomIterator) wrapped).random();
        }

        VolcanoIteratorTupleId volcanoIterator = (pattern.len() < 4) ?
                new VolcanoIteratorTupleId(wrapped, tripleNodeTable, input, output, id):
                new VolcanoIteratorTupleId(wrapped, quadNodeTable, input, output, id);

        // Check if it is a preemptive iterator that should jump directly to its resume state.
        if (!iterators.containsKey(id) && !input.isRandomWalking()) {
            if (input != null && input.getState() != null && input.getState().containsKey(id)) {
                volcanoIterator.skip((SerializableRecord) input.getState(id));
            }
        }
        // iterators.put(id, volcanoIterator); // register and/or erase previous iterator

        return volcanoIterator;
    }
    
}
