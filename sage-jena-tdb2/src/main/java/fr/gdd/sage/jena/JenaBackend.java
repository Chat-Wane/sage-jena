package fr.gdd.sage.jena;

import fr.gdd.sage.interfaces.Backend;
import fr.gdd.sage.interfaces.BackendIterator;
import fr.gdd.sage.generics.LazyIterator;

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
public class JenaBackend implements Backend<NodeId, SerializableRecord> {

    private Dataset dataset;
    private DatasetGraphTDB graph;

    private NodeTupleTable nodeQuadTupleTable;
    private NodeTupleTable nodeTripleTupleTable;
    private NodeTable  nodeTable;
    private PreemptableTupleTable preemptableQuadTupleTable;
    private PreemptableTupleTable preemptableTripleTupleTable;

    public JenaBackend(final String path) {
        dataset = TDB2Factory.connectDataset(path);
        graph = TDBInternal.getDatasetGraphTDB(this.dataset);
        if (!dataset.isInTransaction()) {
            graph.begin();  // opened in at creation
        }

        nodeQuadTupleTable = graph.getQuadTable().getNodeTupleTable();
        nodeTripleTupleTable = graph.getTripleTable().getNodeTupleTable();
        nodeTable  = nodeQuadTupleTable.getNodeTable(); 
        preemptableTripleTupleTable = new PreemptableTupleTable(nodeTripleTupleTable.getTupleTable());
        preemptableQuadTupleTable   = new PreemptableTupleTable(nodeQuadTupleTable.getTupleTable());
    }

    public JenaBackend(final Dataset dataset) {
        this.dataset = dataset;
        graph = TDBInternal.getDatasetGraphTDB(this.dataset);
        if (!dataset.isInTransaction()) {
            graph.begin();  // opened in at creation
        }

        nodeQuadTupleTable = graph.getQuadTable().getNodeTupleTable();
        nodeTripleTupleTable = graph.getTripleTable().getNodeTupleTable();
        nodeTable  = nodeQuadTupleTable.getNodeTable(); 
        preemptableTripleTupleTable = new PreemptableTupleTable(nodeTripleTupleTable.getTupleTable());
        preemptableQuadTupleTable   = new PreemptableTupleTable(nodeQuadTupleTable.getTupleTable());
    }

    /**
     * Needs to be closed this one.
     */
    public void close() { graph.end(); }




    public NodeTable getNodeTable() {
        return nodeTable;
    }

    public NodeTupleTable getNodeTripleTupleTable() {
        return nodeTripleTupleTable;
    }


    
    // Backend interface
    
    @Override
    public BackendIterator<NodeId, SerializableRecord> search(final NodeId s, final NodeId p, final NodeId o, final NodeId... c) {
        if (c.length == 0) {
            Tuple<NodeId> pattern = TupleFactory.tuple(s, p, o);
            return new LazyIterator<NodeId, SerializableRecord>(this, preemptableTripleTupleTable.preemptable_find(pattern));
        } else {
            Tuple<NodeId> pattern = TupleFactory.tuple(c[0], s, p, o);
            return new LazyIterator<NodeId, SerializableRecord>(this, preemptableQuadTupleTable.preemptable_find(pattern));
        }
    }

    @Override
    public NodeId getId(final String value, final int... code) {
        Node node = NodeFactoryExtra.parseNode(value);
        return nodeTable.getNodeIdForNode(node);
    }

    @Override
    public String getValue(final NodeId id, final int... code) {
        Node node = nodeTable.getNodeForNodeId(id);
        return node.toString();
    }

    @Override
    public NodeId any() {
        return NodeId.NodeIdAny;
    }
}
