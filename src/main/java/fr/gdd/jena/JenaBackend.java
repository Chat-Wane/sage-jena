package fr.gdd.jena;

import fr.gdd.Backend;
import fr.gdd.BackendIterator;

import org.apache.jena.atlas.lib.tuple.TupleFactory;
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



public class JenaBackend implements Backend<NodeId, Tuple<NodeId>> {

    private Dataset dataset;
    private DatasetGraphTDB graph;

    private NodeTupleTable node_tuple_table;
    private NodeTable  node_table;
    private PreemptableTupleTable preemptable_tuple_table;
    
    public JenaBackend(final String path) {
        this.dataset = TDB2Factory.connectDataset(path);
        this.graph = TDBInternal.getDatasetGraphTDB(this.dataset);
        this.graph.begin(); // hopefully called close() on drop?

        this.node_tuple_table = this.graph.getTripleTable().getNodeTupleTable();
        this.node_table  = this.node_tuple_table.getNodeTable();
        this.preemptable_tuple_table = new PreemptableTupleTable(node_tuple_table.getTupleTable());
    }


    
    public BackendIterator<NodeId, Tuple<NodeId>> searchIDs(final NodeId s,
                                                            final NodeId p,
                                                            final NodeId o,
                                                            final NodeId c) {
        Tuple<NodeId> pattern = TupleFactory.tuple(s, p, o, c);
        return this.preemptable_tuple_table.find(pattern);
    }

    public NodeId getSubjectID(final String subject) {
        Node subject_node = NodeFactoryExtra.parseNode(subject);
        return node_table.getNodeIdForNode(subject_node);
    }

    public NodeId getPredicateID(final String predicate) {
        Node predicate_node = NodeFactoryExtra.parseNode(predicate);
        return node_table.getNodeIdForNode(predicate_node);
    }

    public NodeId getObjectID(final String object) {
        Node object_node = NodeFactoryExtra.parseNode(object);
        return node_table.getNodeIdForNode(object_node);
    }

    public NodeId getContextID(final String context) {
        Node context_node = NodeFactoryExtra.parseNode(context);
        return node_table.getNodeIdForNode(context_node);
    }

}
