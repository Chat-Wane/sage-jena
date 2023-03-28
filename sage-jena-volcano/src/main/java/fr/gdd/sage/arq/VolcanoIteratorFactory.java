package fr.gdd.sage.arq;

import fr.gdd.sage.interfaces.BackendIterator;
import fr.gdd.sage.io.SageInput;
import fr.gdd.sage.io.SageOutput;
import fr.gdd.sage.jena.PreemptTupleTable;
import fr.gdd.sage.jena.SerializableRecord;
import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.dboe.trans.bplustree.PreemptJenaIterator;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.iterator.PreemptQueryIterUnion;
import org.apache.jena.tdb2.store.DatasetGraphTDB;
import org.apache.jena.tdb2.store.NodeId;
import org.apache.jena.tdb2.store.nodetable.NodeTable;
import org.apache.jena.tdb2.store.nodetupletable.NodeTupleTable;

import java.util.Iterator;
import java.util.List;
import java.util.Map;


/**
 * A volcano iterator factory to ease creation of iterators, one per
 * execution context.
 *
 * (TODO) make it more general to showcase a `ScanIteratorFactory` that would
 * replace the iterator creation of `TDB2`.
 **/
public class VolcanoIteratorFactory {
    
    SageInput<?> input;
    SageOutput<?> output;
    long deadline;

    private NodeTable quadNodeTable;
    private NodeTable tripleNodeTable;
    private PreemptTupleTable preemptableQuadTupleTable;
    private PreemptTupleTable preemptableTripleTupleTable;

    private int unionIds = 1000;
    

    
    public VolcanoIteratorFactory(ExecutionContext context) {
        this.output = context.getContext().get(SageConstants.output);
        this.input  = context.getContext().get(SageConstants.input);

        this.deadline = this.input.getDeadline();
        
        var graph = (DatasetGraphTDB) context.getDataset();
        var nodeQuadTupleTable = graph.getQuadTable().getNodeTupleTable();
        var nodeTripleTupleTable = graph.getTripleTable().getNodeTupleTable();
        quadNodeTable = graph.getQuadTable().getNodeTupleTable().getNodeTable();
        tripleNodeTable = graph.getTripleTable().getNodeTupleTable().getNodeTable();
        preemptableTripleTupleTable = new PreemptTupleTable(nodeTripleTupleTable.getTupleTable());
        preemptableQuadTupleTable   = new PreemptTupleTable(nodeQuadTupleTable.getTupleTable());

    }
    
    public VolcanoIteratorQuad getScan(Tuple<NodeId> pattern, Integer id) {
        BackendIterator<NodeId, SerializableRecord> wrapped = null;
        VolcanoIteratorQuad volcanoIterator = null;
        if (pattern.len() < 4) {
            wrapped = preemptableTripleTupleTable.preemptable_find(pattern);
            volcanoIterator = new VolcanoIteratorQuad(wrapped, tripleNodeTable, input, output, id);
        } else {
            wrapped = preemptableQuadTupleTable.preemptable_find(pattern);
            volcanoIterator = new VolcanoIteratorQuad(wrapped, quadNodeTable, input, output, id);
        }

        if (input != null && input.getState() != null && input.getState().containsKey(id)) {
            volcanoIterator.skip((SerializableRecord) input.getState(id));
        }

        return volcanoIterator;
    }

    public VolcanoIteratorTupleId getScan(NodeTupleTable nodeTupleTable, Tuple<NodeId> pattern, Integer id) {
        BackendIterator<NodeId, SerializableRecord> wrapped = null;
        VolcanoIteratorTupleId volcanoIterator = null;
        if (pattern.len() < 4) {
            wrapped = preemptableTripleTupleTable.preemptable_find(pattern);
            volcanoIterator = new VolcanoIteratorTupleId(wrapped, tripleNodeTable, input, output, id);
        } else {
            wrapped = preemptableQuadTupleTable.preemptable_find(pattern);
            volcanoIterator = new VolcanoIteratorTupleId(wrapped, quadNodeTable, input, output, id);
        }

        // Check if it is a preemptive iterator that should jump directly to its resume state.
        if (input != null && input.getState() != null && input.getState().containsKey(id)) {
            volcanoIterator.skip((SerializableRecord) input.getState(id));

        }

        return volcanoIterator;
    }


    public PreemptQueryIterUnion getUnion(QueryIterator inputIt, List<Op> operations, ExecutionContext context) {
        unionIds += 1;
        PreemptQueryIterUnion it = new PreemptQueryIterUnion(inputIt, operations, context, unionIds);
        if (input != null && input.getState() != null && input.getState().containsKey(unionIds)) {
            it.skip((Integer) input.getState(unionIds));
        }
        return it;
    }
    
}
