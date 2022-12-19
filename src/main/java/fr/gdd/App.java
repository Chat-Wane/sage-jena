package fr.gdd;

import java.util.Iterator;
import java.util.stream.Stream;

import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.query.Dataset;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.tdb2.TDB2Factory;
import org.apache.jena.tdb2.DatabaseMgr;
import org.apache.jena.tdb2.store.DatasetGraphTDB;
import org.apache.jena.tdb2.store.NodeId;
import org.apache.jena.tdb2.store.TripleTable;
import org.apache.jena.tdb2.sys.TDBInternal;



public class App {
    public static void main( String[] args ) {
        System.out.println( "Hello World!" );
        Dataset ds = TDB2Factory.connectDataset("/Users/nedelec-b-2/Desktop/Projects/preemptable-blazegraph/watdiv10M");

        System.out.println("Done connecting to db");
        
        DatasetGraph dsg = ds.asDatasetGraph();
        DatasetGraphTDB tdb = TDBInternal.getDatasetGraphTDB(dsg);
        tdb.begin();

        TripleTable tt =  tdb.getTripleTable();
        Iterator<Tuple<NodeId>> iter = tt.getNodeTupleTable().findAll();

        // iter.forEachRemaining(e -> System.out.println(e));
        long i = 0;
        while (iter.hasNext()) {
            iter.next();
            i += 1;
        };
        
        // s.forEach((e)-> System.out.println(e.getObject().getIndexingValue()));
        
        // long i = s.count();
        tdb.end();
        System.out.println(String.format("%s elements", i));
    }
}
