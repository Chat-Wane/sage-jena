package fr.gdd;

import org.apache.jena.dboe.base.record.Record;
import org.apache.jena.tdb2.store.NodeId;

import fr.gdd.common.BackendIterator;
import fr.gdd.common.SPOC;
import fr.gdd.jena.JenaBackend;
import fr.gdd.jena.JenaIterator;



public class Execute {
    public static void main( String[] args ) {
        JenaBackend b = new JenaBackend("/Users/nedelec-b-2/Desktop/Projects/preemptable-blazegraph/watdiv10M");

        NodeId s = b.getSubjectId("<http://db.uwaterloo.ca/~galuc/wsdbm/Retailer6>");
        BackendIterator<NodeId, Record> it = b.searchIds(s, NodeId.NodeIdAny, NodeId.NodeIdAny, NodeId.NodeIdAny);

        while (it.hasNext()) {
            it.next();
            System.out.printf("%s %s %s \n", it.getId(SPOC.SUBJECT),
                              it.getId(SPOC.PREDICATE),
                              it.getId(SPOC.OBJECT));
        };
        
    }
}
