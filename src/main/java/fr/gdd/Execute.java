package fr.gdd;

import java.util.ArrayList;
import java.util.Arrays;

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
            System.out.printf("%s %s %s \n", it.getValue(SPOC.SUBJECT),
                              it.getValue(SPOC.PREDICATE),
                              it.getValue(SPOC.OBJECT));
        };
        b.close();

        System.out.println("=================================");
        
        query_0();
    }


    public static void query_0() {
        // SELECT ?v1 ?v0 ?v2 ?v4 ?v6 ?v3 WHERE {
	// ?v0 <http://schema.org/eligibleRegion> <http://db.uwaterloo.ca/~galuc/wsdbm/Country21>.
        // ?v0 <http://purl.org/goodrelations/validThrough> ?v3.
        // ?v0 <http://purl.org/goodrelations/includes> ?v1.
        // ?v1 <http://schema.org/text> ?v6.
        // ?v0 <http://schema.org/eligibleQuantity> ?v4.
        // ?v0 <http://purl.org/goodrelations/price> ?v2. }

        JenaBackend b = new JenaBackend("/Users/nedelec-b-2/Desktop/Projects/preemptable-blazegraph/watdiv10M");
        NodeId p_1 = b.getPredicateId("<http://schema.org/eligibleRegion>");
        NodeId o_1 = b.getObjectId("<http://db.uwaterloo.ca/~galuc/wsdbm/Country21>");
        NodeId p_2 = b.getPredicateId("<http://purl.org/goodrelations/validThrough>");
        NodeId p_3 = b.getPredicateId("<http://purl.org/goodrelations/includes>");
        NodeId p_4 = b.getPredicateId("<http://schema.org/text>");
        NodeId p_5 = b.getPredicateId("<http://schema.org/eligibleQuantity>");
        NodeId p_6 = b.getPredicateId("<http://purl.org/goodrelations/price>");
        NodeId any = NodeId.NodeIdAny;

        ArrayList<ArrayList<String>> results = new ArrayList();
        BackendIterator<NodeId, Record> it_1 = b.searchIds(any, p_1, o_1, any);
        while (it_1.hasNext()) {
            it_1.next();
            results.add(new ArrayList<String>(Arrays.asList(it_1.getValue(SPOC.SUBJECT))));
        }

        System.out.printf("%s results\n", results.size());

    }
    
}
