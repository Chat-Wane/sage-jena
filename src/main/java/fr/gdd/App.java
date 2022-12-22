package fr.gdd;

import java.util.Iterator;
import java.util.stream.Stream;

import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.atlas.lib.tuple.TupleFactory;
import org.apache.jena.dboe.base.record.Record;
import org.apache.jena.dboe.trans.bplustree.BPTreeNode;
import org.apache.jena.dboe.trans.bplustree.BPTreePage;
import org.apache.jena.dboe.trans.bplustree.BPlusTree;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.Dataset;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.tdb2.TDB2Factory;
import org.apache.jena.tdb2.DatabaseMgr;
import org.apache.jena.tdb2.store.DatasetGraphTDB;
import org.apache.jena.tdb2.store.NodeId;
import org.apache.jena.tdb2.store.TripleTable;
import org.apache.jena.tdb2.store.nodetable.NodeTable;
import org.apache.jena.tdb2.store.tupletable.TupleIndex;
import org.apache.jena.tdb2.store.tupletable.TupleIndexRecord;
import org.apache.jena.tdb2.store.tupletable.TupleTable;
import org.apache.jena.tdb2.store.nodetupletable.NodeTupleTable;
import org.apache.jena.tdb2.sys.TDBInternal;



public class App {
    public static void main( String[] args ) {
        System.out.println( "Opening TDB2 !" );
        long start = System.currentTimeMillis();
        Dataset ds = TDB2Factory.connectDataset("/Users/nedelec-b-2/Desktop/Projects/preemptable-blazegraph/watdiv10M");
        long end = System.currentTimeMillis();

        System.out.printf("Took %s ms to open TDB.\n", end-start);
        
        DatasetGraph dsg = ds.asDatasetGraph();
        DatasetGraphTDB tdb = TDBInternal.getDatasetGraphTDB(dsg);

        start = System.currentTimeMillis();
        tdb.begin();

        TripleTable tt =  tdb.getTripleTable();
        NodeTupleTable ntt = tt.getNodeTupleTable();
        NodeTable node_table = ntt.getNodeTable();

        // Node uri = NodeFactory.createURI("http://db.uwaterloo.ca/~galuc/wsdbm/City193");
        Node uri = NodeFactory.createURI("http://db.uwaterloo.ca/~galuc/wsdbm/Retailer6");
        NodeId uri_id = node_table.getNodeIdForNode(uri);
        System.out.printf("%s \n", uri_id);
        
        // Tuple<NodeId> pattern = TupleFactory.tuple(uri_id, NodeId.NodeIdAny, NodeId.NodeIdAny);

        Iterator<Tuple<NodeId>> uri_ids = ntt.find(uri_id, NodeId.NodeIdAny, NodeId.NodeIdAny);
        Tuple<NodeId> found = null;
        int j = 0;
        while (uri_ids.hasNext()) {
            found = uri_ids.next();
            j += 1;
            System.out.printf("%s: %s \n", j, found);
        };
        
        Node predicate = node_table.getNodeForNodeId(found.get(1));
        Node object = node_table.getNodeForNodeId(found.get(2));        
        System.out.printf("%s %s \n", predicate, object);
        
        TupleTable tuple_table = ntt.getTupleTable();

        // TODO get the proper index: <https://github.com/apache/jena/blob/c4e999d633b2532b504b35937db4bec9a7c2e539/jena-tdb2/src/main/java/org/apache/jena/tdb2/store/tupletable/TupleTable.java#L133>
        
        TupleIndex ti = tuple_table.getIndex(0);
        // See to create an iterator: <https://github.com/apache/jena/blob/ebc10c4131726e25f6ffd398b9d7a0708aac8066/jena-tdb2/src/main/java/org/apache/jena/tdb2/store/tupletable/TupleIndexRecord.java#L202>
        // it uses RangeIndex. We can get rangeindex by calling getRangeIndex on TupleIndexRecord that
        // is a cast of tuple index. 
        // We can even have BPlusTree from that Range index
        // see : <https://github.com/apache/jena/blob/ebc10c4131726e25f6ffd398b9d7a0708aac8066/jena-tdb2/src/main/java/org/apache/jena/tdb2/loader/base/LoaderOps.java#L69>

        // Then https://github.com/apache/jena/blob/31dc0d328c4858401e5d3fa99702c97eba0383a0/jena-db/jena-dboe-trans-data/src/main/java/org/apache/jena/dboe/trans/bplustree/BPTreeRangeIterator.java

        // we can get node using access path... <https://github.com/apache/jena/blob/31dc0d328c4858401e5d3fa99702c97eba0383a0/jena-db/jena-dboe-trans-data/src/main/java/org/apache/jena/dboe/trans/bplustree/AccessPath.java>

        
        // (Not sure useful now but=> especially take a look at recordMapper: <https://github.com/apache/jena/blob/ebc10c4131726e25f6ffd398b9d7a0708aac8066/jena-tdb2/src/main/java/org/apache/jena/tdb2/store/tupletable/TupleIndexRecord.java#L62>
        
        
        // BPTreePage bptp = null;
        // bptp.getId();
        // BPTreeNode bptn = null;
        // bptn.getCount();
        // bptn.getId();
        // bptn.search(root, record);
        // BPlusTree bpt = null;
        // bpt.find(record);

        System.out.println();
        
        TupleIndexRecord tir = (TupleIndexRecord) ti;

        PreemptableTupleIndexRecord ptir = new PreemptableTupleIndexRecord(tir);
        var tuple = TupleFactory.create3(uri_id, NodeId.NodeIdAny, NodeId.NodeIdAny);
        BPTreePreemptableRangeIterator it = (BPTreePreemptableRangeIterator) ptir.scan(tuple);

        int limit = 0;
        Record skip_to = null;
        while (it.hasNext()) {
            var meow = it.next();
            limit += 1;
            System.out.printf("%s: %s \n", limit, meow);
            if (limit == 10) {
                skip_to = it.current();
                break;
            }
        };

        System.out.println("Resuming…");
        
        BPTreePreemptableRangeIterator it2 = (BPTreePreemptableRangeIterator) ptir.scan(tuple);
        it2.skip(skip_to);
        while (it2.hasNext()) {
            var meow = it2.next();
            limit += 1;
            System.out.printf("%s: %s \n", limit, meow);
        }



        BPTreePreemptableRangeIterator it3 = (BPTreePreemptableRangeIterator) ptir.scan(tuple);
        System.out.printf("card: %s \n", it3.cardinality());
                

         
        Iterator<Tuple<NodeId>> iter = ntt.findAll();

        // iter.forEachRemaining(e -> System.out.println(e));
        long i = 0;
        while (iter.hasNext()) {
            iter.next();
            i += 1;
        };

        
        tdb.end();
        end = System.currentTimeMillis();
        System.out.println(String.format("%s elements in %s ms", i, end - start));
    }
}
