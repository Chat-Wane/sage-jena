package fr.gdd.sage.jena;

import fr.gdd.sage.generics.LazyIterator;
import fr.gdd.sage.interfaces.Backend;
import fr.gdd.sage.interfaces.BackendIterator;
import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.atlas.lib.tuple.TupleFactory;
import org.apache.jena.graph.Node;
import org.apache.jena.query.Dataset;
import org.apache.jena.shared.NotFoundException;
import org.apache.jena.sparql.util.NodeFactoryExtra;
import org.apache.jena.tdb2.TDB2Factory;
import org.apache.jena.tdb2.store.DatasetGraphTDB;
import org.apache.jena.tdb2.store.NodeId;
import org.apache.jena.tdb2.store.nodetable.NodeTable;
import org.apache.jena.tdb2.store.nodetupletable.NodeTupleTable;
import org.apache.jena.tdb2.sys.TDBInternal;

import java.util.Objects;


/**
 * TDB2 Jena Backend implementation of the interface `Backend<ID,
 * SKIP>`.
 **/
public class JenaBackend implements Backend<NodeId, SerializableRecord> {

    Dataset dataset;
    DatasetGraphTDB graph;

    NodeTupleTable nodeQuadTupleTable;
    NodeTupleTable nodeTripleTupleTable;
    NodeTable  nodeTripleTable;
    NodeTable  nodeQuadTable;
    PreemptableTupleTable preemptableTripleTupleTable;
    PreemptableTupleTable preemptableQuadTupleTable;


    public JenaBackend(final String path) {
        dataset = TDB2Factory.connectDataset(path);
        graph = TDBInternal.getDatasetGraphTDB(this.dataset);
        if (!dataset.isInTransaction()) {
            graph.begin();  // opened in at creation
        }
        loadDataset();
    }

    public JenaBackend(final Dataset dataset) {
        this.dataset = dataset;
        graph = TDBInternal.getDatasetGraphTDB(this.dataset);
        if (!dataset.isInTransaction()) {
            graph.begin();  // opened in at creation
        }
        loadDataset();
    }

    private void loadDataset() {
        nodeTripleTupleTable = graph.getTripleTable().getNodeTupleTable();
        nodeQuadTupleTable = graph.getQuadTable().getNodeTupleTable();
        nodeTripleTable = nodeTripleTupleTable.getNodeTable();
        nodeQuadTable  = nodeQuadTupleTable.getNodeTable();
        preemptableTripleTupleTable = new PreemptableTupleTable(nodeTripleTupleTable.getTupleTable());
        preemptableQuadTupleTable   = new PreemptableTupleTable(nodeQuadTupleTable.getTupleTable());
    }

    /**
     * Needs to be closed this one.
     */
    public void close() { graph.end(); }



    
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
    public NodeId getId(final String value, final int... code) throws NotFoundException {
        Node node = NodeFactoryExtra.parseNode(value);
        NodeId id = nodeTripleTable.getNodeIdForNode(node);
        if (NodeId.isDoesNotExist(id)) {
            id = nodeQuadTable.getNodeIdForNode(node);
            if (NodeId.isDoesNotExist(id)) {
                throw new NotFoundException(String.format("Id of %s does not exist.", value));
            }
        }
        return id;
    }

    @Override
    public String getValue(final NodeId id, final int... code) throws NotFoundException {
        Node node = nodeTripleTable.getNodeForNodeId(id);
        if (Objects.isNull(node)) {
            node = nodeQuadTable.getNodeForNodeId(id);
            if (Objects.isNull(node)) {
                throw new NotFoundException(String.format("Id of %s does not exist.", id.toString()));
            }
        }
        return node.toString();
    }

    @Override
    public NodeId any() {
        return NodeId.NodeIdAny;
    }
}