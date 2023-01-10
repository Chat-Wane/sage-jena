package fr.gdd.jena;

import fr.gdd.common.Backend;
import fr.gdd.common.BackendIterator;
import fr.gdd.common.LazyIterator;
import fr.gdd.common.RandomIterator;

import org.apache.jena.atlas.lib.tuple.TupleFactory;
import org.apache.jena.dboe.base.record.Record;

import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.query.Dataset;
import org.apache.jena.tdb2.store.DatasetGraphTDB;
import org.apache.jena.tdb2.TDB2Factory;
import org.apache.jena.tdb2.sys.TDBInternal;
import org.apache.jena.tdb2.store.NodeId;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.util.NodeFactoryExtra;
import org.apache.jena.tdb2.store.nodetupletable.NodeTupleTable;
import org.apache.jena.tdb2.store.nodetable.NodeTable;



/**
 * TDB2 Jena Backend implementation of the interface `Backend<ID,
 * SKIP>`.
 **/
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
        graph.begin();  // opened in at creation

        node_quad_tuple_table = graph.getQuadTable().getNodeTupleTable();
        node_triple_tuple_table = graph.getTripleTable().getNodeTupleTable();
        node_table  = node_quad_tuple_table.getNodeTable(); 
        preemptable_triple_tuple_table = new PreemptableTupleTable(node_triple_tuple_table.getTupleTable());
        preemptable_quad_tuple_table   = new PreemptableTupleTable(node_quad_tuple_table.getTupleTable());
    }

    /**
     * Needs to be closed this one.
     */
    public void close() { graph.end(); }



    // Backend interface
    
    @Override
    public BackendIterator<NodeId, Record> search(final NodeId s, final NodeId p, final NodeId o, final NodeId... c) {
        if (c.length == 0) {
            Tuple<NodeId> pattern = TupleFactory.tuple(s, p, o);
            return new LazyIterator<NodeId, Record>(this, preemptable_triple_tuple_table.preemptable_find(pattern));
        } else {
            Tuple<NodeId> pattern = TupleFactory.tuple(c[0], s, p, o);
            return new LazyIterator<NodeId, Record>(this, preemptable_quad_tuple_table.preemptable_find(pattern));
        }
    }

    @Override
    public NodeId getId(final String value, final int... code) {
        Node node = NodeFactoryExtra.parseNode(value);
        return node_table.getNodeIdForNode(node);
    }

    @Override
    public String getValue(final NodeId id, final int... code) {
        Node node = node_table.getNodeForNodeId(id);
        return node.toString();
    }
}
