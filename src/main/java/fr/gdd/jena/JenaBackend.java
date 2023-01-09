package fr.gdd.jena;

import fr.gdd.common.Backend;
import fr.gdd.common.BackendIterator;
import fr.gdd.common.LazyIterator;

import org.apache.jena.atlas.lib.tuple.TupleFactory;
import org.apache.jena.dboe.base.record.Record;

import java.util.Iterator;
import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.query.Dataset;
import org.apache.jena.tdb2.store.DatasetGraphTDB;
import org.apache.jena.tdb2.TDB2Factory;
import org.apache.jena.tdb2.sys.TDBInternal;
import org.apache.jena.tdb2.store.NodeId;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.util.NodeFactoryExtra;
import org.apache.jena.tdb2.store.tupletable.TupleTable;
import org.apache.jena.tdb2.store.nodetupletable.NodeTupleTable;
import org.apache.jena.tdb2.store.nodetable.NodeTable;



public class JenaBackend implements Backend<NodeId, Record> {

    private Dataset dataset;
    private DatasetGraphTDB graph;

    private NodeTupleTable node_quad_tuple_table;
    private NodeTupleTable node_triple_tuple_table;
    private NodeTable  node_table;
    private PreemptableTupleTable preemptable_quad_tuple_table;
    private PreemptableTupleTable preemptable_triple_tuple_table;
    
    public JenaBackend(final String path) {
        dataset = TDB2Factory.connectDataset(path);
        graph = TDBInternal.getDatasetGraphTDB(this.dataset);
        graph.begin(); 

        node_quad_tuple_table = graph.getQuadTable().getNodeTupleTable();
        node_triple_tuple_table = graph.getTripleTable().getNodeTupleTable();
        node_table  = node_quad_tuple_table.getNodeTable(); 
        preemptable_triple_tuple_table = new PreemptableTupleTable(node_triple_tuple_table.getTupleTable());
        preemptable_quad_tuple_table   = new PreemptableTupleTable(node_quad_tuple_table.getTupleTable());
    }

    /**
     * Needs to be closed this one.
     */
    public void close() {
        graph.end();
    }


    
    public BackendIterator<NodeId, Record> searchIds(final NodeId s,
                                                     final NodeId p,
                                                     final NodeId o,
                                                     final NodeId c) {
        Tuple<NodeId> pattern = null;
        if (c == null) {
            pattern = TupleFactory.tuple(s, p, o);
            return new LazyIterator<NodeId, Record>(this, preemptable_triple_tuple_table.preemptable_find(pattern));
        } else {
            pattern = TupleFactory.tuple(c, s, p, o);
            return new LazyIterator<NodeId, Record>(this, preemptable_quad_tuple_table.preemptable_find(pattern));
        }

    }

    public NodeId getSubjectId(final String subject) {
        Node subject_node = NodeFactoryExtra.parseNode(subject);
        return node_table.getNodeIdForNode(subject_node);
    }

    public NodeId getPredicateId(final String predicate) {
        Node predicate_node = NodeFactoryExtra.parseNode(predicate);
        return node_table.getNodeIdForNode(predicate_node);
    }

    public NodeId getObjectId(final String object) {
        Node object_node = NodeFactoryExtra.parseNode(object);
        return node_table.getNodeIdForNode(object_node);
    }

    public NodeId getContextId(final String context) {
        Node context_node = NodeFactoryExtra.parseNode(context);
        return node_table.getNodeIdForNode(context_node);
    }

    public String getValue(final NodeId id) {
        Node node = node_table.getNodeForNodeId(id);
        return node.toString();
    }

}
