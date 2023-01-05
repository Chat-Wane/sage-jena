package fr.gdd;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.dboe.base.record.Record;
import org.apache.jena.tdb2.store.NodeId;

import fr.gdd.common.BackendIterator;
import fr.gdd.common.SPOC;
import fr.gdd.common.SageInput;
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
        
        query_0(new SageInput<Record>(100));
    }


    public static void query_0(SageInput<Record> input) {
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

        ArrayList<ArrayList<String>> results = new ArrayList<>();
        
        // ?v0 <http://schema.org/eligibleRegion> <http://db.uwaterloo.ca/~galuc/wsdbm/Country21>.
        BackendIterator<NodeId, Record> it_1 = b.searchIds(any, p_1, o_1, any);
        while (it_1.hasNext()) {
            it_1.next();
            // ?v0 <http://purl.org/goodrelations/validThrough> ?v3.
            BackendIterator<NodeId, Record> it_2 = b.searchIds(it_1.getId(SPOC.SUBJECT), p_2, any, any);
            while (it_2.hasNext()) {
                it_2.next();
                // ?v0 <http://purl.org/goodrelations/includes> ?v1.
                BackendIterator<NodeId, Record> it_3 = b.searchIds(it_1.getId(SPOC.SUBJECT), p_3, any, any);
                while (it_3.hasNext()) {
                    it_3.next();
                    // ?v1 <http://schema.org/text> ?v6.
                    var it_4 = b.searchIds(it_3.getId(SPOC.OBJECT), p_4, any, any);
                    while (it_4.hasNext()) {
                        it_4.next();
                        // ?v0 <http://schema.org/eligibleQuantity> ?v4.
                        var it_5 = b.searchIds(it_1.getId(SPOC.SUBJECT), p_5, any, any);
                        while (it_5.hasNext()) {
                            it_5.next();
                            // ?v0 <http://purl.org/goodrelations/price> ?v2.
                            var it_6 = b.searchIds(it_1.getId(SPOC.SUBJECT), p_6, any, any);
                            while (it_6.hasNext()) {
                                it_6.next();
                                results.add(new ArrayList<String>(Arrays.asList(it_1.getValue(SPOC.SUBJECT),
                                                                                it_2.getValue(SPOC.OBJECT),
                                                                                it_3.getValue(SPOC.OBJECT),
                                                                                it_4.getValue(SPOC.OBJECT),
                                                                                it_5.getValue(SPOC.OBJECT),
                                                                                it_6.getValue(SPOC.OBJECT))));
                                if (results.size() >= input.getLimit()) {
                                    input.setState(new Pair(1, it_1.previous()),
                                                   new Pair(2, it_2.previous()),
                                                   new Pair(3, it_3.previous()),
                                                   new Pair(4, it_4.previous()),
                                                   new Pair(5, it_5.previous()),
                                                   new Pair(6, it_6.current()));
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }

        // expect 326 results
        System.out.printf("%s results\n", results.size());

    }
    
}
