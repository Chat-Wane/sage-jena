package fr.gdd;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.dboe.base.record.Record;
import org.apache.jena.tdb2.store.NodeId;

import fr.gdd.common.BackendIterator;
import fr.gdd.common.SPOC;
import fr.gdd.common.SageInput;
import fr.gdd.common.SageOutput;
import fr.gdd.jena.JenaBackend;
import fr.gdd.jena.JenaIterator;



public class Execute {
    public static void main( String[] args ) {

        // JenaBackend b_42 = new JenaBackend("/Users/nedelec-b-2/Desktop/Projects/preemptable-blazegraph/watdiv42M");
       
        // System.out.println("=================================");

        // NodeId s_1_42 = b_42.getSubjectId("<http://www.person0.fr/Review15>");
        // // NodeId p_1_42 = b_42.getPredicateId("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>");
        NodeId any = NodeId.NodeIdAny;
        
        // BackendIterator<NodeId, Record> it_1_42 = b_42.searchIds(s_1_42, any, any, any);
        // long sum_42 = 0;
        // while (it_1_42.hasNext()) {
        //     it_1_42.next();
        //     System.out.printf("ALL %s %s %s %s \n", it_1_42.getId(SPOC.SUBJECT),
        //                       it_1_42.getId(SPOC.PREDICATE),
        //                       it_1_42.getId(SPOC.OBJECT),
        //                       it_1_42.getId(SPOC.GRAPH));
        //     sum_42 += 1;
        // }
        // System.out.printf("CARD = %s\n", it_1_42.cardinality());
        // System.out.printf("ALL SIZE = %s\n", sum_42);
        // System.out.println("=================================");

        // System.exit(1);

        
        JenaBackend b = new JenaBackend("/Users/nedelec-b-2/Desktop/Projects/preemptable-blazegraph/watdiv10M");

        // NodeId s = b.getSubjectId("<http://db.uwaterloo.ca/~galuc/wsdbm/Retailer6>");
        // BackendIterator<NodeId, Record> it = b.searchIds(s, NodeId.NodeIdAny, NodeId.NodeIdAny, NodeId.NodeIdAny);

        // while (it.hasNext()) {
        //     it.next();
        //     System.out.printf("%s %s %s \n", it.getId(SPOC.SUBJECT),
        //                       it.getId(SPOC.PREDICATE),
        //                       it.getId(SPOC.OBJECT));
        //     System.out.printf("%s %s %s \n", it.getValue(SPOC.SUBJECT),
        //                       it.getValue(SPOC.PREDICATE),
        //                       it.getValue(SPOC.OBJECT));
        // };
        // b.close();

        
        System.out.println("=================================");

        NodeId p_1 = b.getId("<http://schema.org/eligibleRegion>");
        NodeId o_1 = b.getId("<http://db.uwaterloo.ca/~galuc/wsdbm/Country21>");
        // NodeId any = NodeId.NodeIdAny;
        
        BackendIterator<NodeId, Record> it_1 = b.search(any, p_1, o_1);
        long sum = 0;
        while (it_1.hasNext()) {
            it_1.next();
            System.out.printf("ALL %s %s %s \n", it_1.getId(SPOC.SUBJECT),
                              it_1.getId(SPOC.PREDICATE),
                              it_1.getId(SPOC.OBJECT));
            sum+=1;
        }
        System.out.printf("CARD = %s\n", it_1.cardinality());
        System.out.printf("ALL SIZE = %s\n", sum);
        System.out.println("=================================");


        
        SageOutput<Record> results = null;
        SageOutput<Record> fullResults = new SageOutput<>();
        var input = new SageInput<Record>(10);
        input.setBackend(b);
        sum = 0;
        while (results == null || results.getState() != null) {
            sum +=1;
            results = query_0(input);
            fullResults.merge(results);
            input.setState(results.getState());
            System.out.printf("%s \n", results.getState());
            System.out.printf("%s results\n", results.size());
        };
        System.out.printf("FINAL SIZE = %s\n" , fullResults.size());
    }


    public static SageOutput<Record> query_0_simple(SageInput<Record> input) {
        JenaBackend b = (JenaBackend) input.getBackend();
        NodeId p_1 = b.getId("<http://schema.org/eligibleRegion>");
        NodeId o_1 = b.getId("<http://db.uwaterloo.ca/~galuc/wsdbm/Country21>");
        NodeId any = NodeId.NodeIdAny;
        SageOutput<Record> results = new SageOutput<>();
        
        boolean once_1 = false;
        BackendIterator<NodeId, Record> it_1 = b.search(any, p_1, o_1);
        if (!once_1) {
            once_1 = true;
            var to = input.getState(1);
            System.out.printf("starting over at %s\n", to);
            it_1.skip(to);
        }
        while (it_1.hasNext()) {
            it_1.next();
            results.addResult(new ArrayList<String>(Arrays.asList(it_1.getValue(SPOC.SUBJECT))));
            System.out.printf("PREEMPT %s %s %s \n", it_1.getId(SPOC.SUBJECT),
                              it_1.getId(SPOC.PREDICATE),
                              it_1.getId(SPOC.OBJECT));
            if (results.size() >= input.getLimit()) {
                results.save(new Pair(1, it_1.current()));
                return results;
            }
        }
        return results;
    }
    

    public static SageOutput<Record> query_0(SageInput<Record> input) {
        // SELECT ?v1 ?v0 ?v2 ?v4 ?v6 ?v3 WHERE {
	// ?v0 <http://schema.org/eligibleRegion> <http://db.uwaterloo.ca/~galuc/wsdbm/Country21>.
        // ?v0 <http://purl.org/goodrelations/validThrough> ?v3.
        // ?v0 <http://purl.org/goodrelations/includes> ?v1.
        // ?v1 <http://schema.org/text> ?v6.
        // ?v0 <http://schema.org/eligibleQuantity> ?v4.
        // ?v0 <http://purl.org/goodrelations/price> ?v2. }

             
        // JenaBackend b = new JenaBackend("/Users/nedelec-b-2/Desktop/Projects/preemptable-blazegraph/watdiv10M");
        JenaBackend b = (JenaBackend) input.getBackend();
        NodeId p_1 = b.getId("<http://schema.org/eligibleRegion>");
        NodeId o_1 = b.getId("<http://db.uwaterloo.ca/~galuc/wsdbm/Country21>");
        NodeId p_2 = b.getId("<http://purl.org/goodrelations/validThrough>");
        NodeId p_3 = b.getId("<http://purl.org/goodrelations/includes>");
        NodeId p_4 = b.getId("<http://schema.org/text>");
        NodeId p_5 = b.getId("<http://schema.org/eligibleQuantity>");
        NodeId p_6 = b.getId("<http://purl.org/goodrelations/price>");
        NodeId any = NodeId.NodeIdAny;

        // ArrayList<ArrayList<String>> results = new ArrayList<>();
        SageOutput<Record> results = new SageOutput<>();
        
        boolean once_1 = false;
        boolean once_2 = false;
        boolean once_3 = false;
        boolean once_4 = false;
        boolean once_5 = false;
        boolean once_6 = false;

        // ?v0 <http://schema.org/eligibleRegion> <http://db.uwaterloo.ca/~galuc/wsdbm/Country21>.
        BackendIterator<NodeId, Record> it_1 = b.search(any, p_1, o_1);
        if (!once_1) {
            once_1 = true;
            it_1.skip(input.getState(1));
        }
        while (it_1.hasNext()) {
            it_1.next();
            // ?v0 <http://purl.org/goodrelations/validThrough> ?v3.
            BackendIterator<NodeId, Record> it_2 = b.search(it_1.getId(SPOC.SUBJECT), p_2, any);
            if (!once_2) {
                once_2 = true;
                it_2.skip(input.getState(2));
            }
            while (it_2.hasNext()) {
                it_2.next();
                // ?v0 <http://purl.org/goodrelations/includes> ?v1.
                BackendIterator<NodeId, Record> it_3 = b.search(it_1.getId(SPOC.SUBJECT), p_3, any);
                if (!once_3) {
                    once_3 = true;
                    it_3.skip(input.getState(3));
                }
                
                while (it_3.hasNext()) {
                    it_3.next();
                    // ?v1 <http://schema.org/text> ?v6.
                    var it_4 = b.search(it_3.getId(SPOC.OBJECT), p_4, any);
                    if (!once_4) {
                        once_4 = true;
                        it_4.skip(input.getState(4));
                    }

                    while (it_4.hasNext()) {
                        it_4.next();
                        // ?v0 <http://schema.org/eligibleQuantity> ?v4.
                        var it_5 = b.search(it_1.getId(SPOC.SUBJECT), p_5, any);
                        if (!once_5) {
                            once_5 = true;
                            it_5.skip(input.getState(5));
                        }
                        
                        while (it_5.hasNext()) {
                            it_5.next();

                            // ?v0 <http://purl.org/goodrelations/price> ?v2.
                            var it_6 = b.search(it_1.getId(SPOC.SUBJECT), p_6, any);
                            if (!once_6) {
                                once_6 = true;
                                it_6.skip(input.getState(6));
                            }
                            
                            while (it_6.hasNext()) {
                                it_6.next();
                                results.addResult(new ArrayList<String>(Arrays.asList(it_1.getValue(SPOC.SUBJECT),
                                                                                      it_2.getValue(SPOC.OBJECT),
                                                                                      it_3.getValue(SPOC.OBJECT),
                                                                                      it_4.getValue(SPOC.OBJECT),
                                                                                      it_5.getValue(SPOC.OBJECT),
                                                                                      it_6.getValue(SPOC.OBJECT))));
                                if (results.size() >= input.getLimit()) {
                                    results.save(new Pair(1, it_1.previous()),
                                                 new Pair(2, it_2.previous()),
                                                 new Pair(3, it_3.previous()),
                                                 new Pair(4, it_4.previous()),
                                                 new Pair(5, it_5.previous()),
                                                 new Pair(6, it_6.current()));
                                    return results;
                                }
                            }
                        }
                    }
                }
            }
        }

        // expect 326 results
        System.out.printf("%s results\n", results.size());
        return results;
    }
    
}
