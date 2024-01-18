package org.apache.jena.dboe.trans.bplustree;

import fr.gdd.sage.ArtificallySkewedGraph;
import fr.gdd.sage.databases.inmemory.InMemoryInstanceOfTDB2;
import fr.gdd.sage.databases.persistent.Watdiv10M;
import fr.gdd.sage.generics.LazyIterator;
import fr.gdd.sage.generics.Pair;
import fr.gdd.sage.interfaces.BackendIterator;
import fr.gdd.sage.interfaces.SPOC;
import fr.gdd.sage.io.SageOutput;
import fr.gdd.sage.jena.JenaBackend;
import fr.gdd.sage.jena.SerializableRecord;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.base.Sys;
import org.apache.jena.dboe.base.record.Record;
import org.apache.jena.query.Dataset;
import org.apache.jena.tdb2.store.Hash;
import org.apache.jena.tdb2.store.NodeId;
import org.apache.jena.tdb2.sys.TDBInternal;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ProgressJenaIteratorTest {

    Logger log = LoggerFactory.getLogger(ProgressJenaIteratorTest.class);

    static Dataset dataset = null;
    static JenaBackend backend = null;

    static NodeId predicate = null;
    static NodeId any = null;

    @BeforeAll
    public static void initializeDB() {
        dataset = new InMemoryInstanceOfTDB2().getDataset();

        backend = new JenaBackend(dataset);
        predicate = backend.getId("<http://www.geonames.org/ontology#parentCountry>");
        any = backend.any();
    }

    @AfterAll
    public static void closeDB() {
        dataset.abort();
        TDBInternal.expel(dataset.asDatasetGraph());
    }

    /**
     * Function that ease the creation of two loops, it stops at the `stopAt`^th next();
     */
    private static SageOutput run_loops(JenaBackend backend, HashMap<Integer, Serializable> savedState, int stopAt) {
        NodeId city_2 = backend.getId("<http://db.uwaterloo.ca/~galuc/wsdbm/City102>");
        BackendIterator<NodeId, Serializable> it = backend.search(city_2, any, any);
        SageOutput<Serializable> output = new SageOutput<>();
        if (savedState.containsKey(0)) {
            it.skip(savedState.get(0));
        }

        int shouldStop = 0;
        while (it.hasNext()) {
            it.next();

            BackendIterator<?, Serializable> it_2 = backend.search(any, it.getId(SPOC.PREDICATE), any);
            if (savedState.containsKey(1)) {
                it_2.skip(savedState.get(1));
            }
            while (it_2.hasNext()) {
                it_2.next();

                shouldStop += 1;
                if (shouldStop >= stopAt) {
                    output.save(new Pair<>(0, it.previous()), new Pair<>(1, it_2.current()));
                    break;
                }
            }
            shouldStop += 1;
            if (shouldStop >= stopAt) {
                output.save(new Pair<>(0, it.current()));
                break;
            }
        }

        return output;
    }

    /* ********************************************************************************************* */
    // Since we don't know for sure the error on cardinality, we cannot set appropriate assertions


    @Disabled
    @Test
    public void cardinality_that_seems_not_good_watdiv() {
        // Comes from the observation that with 2k random walks, we
        // converge towards a cardinality that is not good
        // [main] DEBUG fr.gdd.sage.arq.SageOptimizer - triple ?v0 @http://xmlns.com/foaf/familyName ?v1 => 64861 elements
        // [main] DEBUG fr.gdd.sage.arq.SageOptimizer - triple ?v0 @http://xmlns.com/foaf/givenName ?v2 => 68338 elements
        new Watdiv10M(Optional.of("../target"));
        JenaBackend backend = new JenaBackend("../target/watdiv10M");
        NodeId family = backend.getId("<http://xmlns.com/foaf/familyName>");
        NodeId given = backend.getId("<http://xmlns.com/foaf/givenName>");

        PreemptJenaIterator.NB_WALKS = 200000;

        var it = backend.search(backend.any(), given, backend.any());
        ProgressJenaIterator casted = (ProgressJenaIterator) ((LazyIterator) it).iterator;
        assertEquals(69970, casted.cardinality(Integer.MAX_VALUE));
        log.info("Expected 69970, got {}.", casted.cardinality());

        it = backend.search(backend.any(), family, backend.any());
        casted = (ProgressJenaIterator) ((LazyIterator) it).iterator;
        log.info("Expected 69970, got {}.", casted.cardinality());
        assertEquals(69970, casted.cardinality(Integer.MAX_VALUE));
    }


    @Disabled
    @Test
    public void cardinality_of_larger_triple_pattern_above_leaf_size_with_watdiv_with_query1000() {
        JenaBackend backend = new JenaBackend("../target/watdiv10M");
        // OpBGP op = (OpBGP) SSE.parseOp("(bgp (?v0 <http://schema.org/eligibleRegion> <http://db.uwaterloo.ca/~galuc/wsdbm/Country21>))"); // expect 2613
        // OpBGP op = (OpBGP) SSE.parseOp("(bgp (?v0 <http://purl.org/goodrelations/validThrough> ?v3))"); // expect 36346
        // OpBGP op = (OpBGP) SSE.parseOp("(bgp (?v0 <http://purl.org/goodrelations/includes> ?v1))"); // expect 90000
        // OpBGP op = (OpBGP) SSE.parseOp("(bgp (?v1 <http://schema.org/text> ?v6))"); // expect 7476
        // OpBGP op = (OpBGP) SSE.parseOp("(bgp (?v0 <http://schema.org/eligibleQuantity> ?v4))"); // expect 90000
        // OpBGP op = (OpBGP) SSE.parseOp("(bgp (?v0 <http://purl.org/goodrelations/price> ?v2))"); // expect 240000

        NodeId price = backend.getId("<http://purl.org/goodrelations/price>");
        var it = backend.search(backend.any(), price, backend.any());
        ProgressJenaIterator casted = (ProgressJenaIterator) ((LazyIterator) it).iterator;
        assertEquals(240000, casted.cardinality(Integer.MAX_VALUE));
        log.info("Expected 240000, got {}.", casted.cardinality(200000));

        NodeId eligible = backend.getId("<http://schema.org/eligibleQuantity>");
        it = backend.search(backend.any(), eligible, backend.any());
        casted = (ProgressJenaIterator) ((LazyIterator) it).iterator;
        assertEquals(90000, casted.cardinality(Integer.MAX_VALUE));
        log.info("Expected 90000, got {}.", casted.cardinality(200000));

        NodeId text = backend.getId("<http://schema.org/text>");
        it = backend.search(backend.any(), text, backend.any());
        casted = (ProgressJenaIterator) ((LazyIterator) it).iterator;
        assertEquals(7476, casted.cardinality(Integer.MAX_VALUE));
        log.info("Expected 7476, got {}.", casted.cardinality(200000));

        NodeId include = backend.getId("<http://purl.org/goodrelations/includes>");
        it = backend.search(backend.any(), include, backend.any());
        casted = (ProgressJenaIterator) ((LazyIterator) it).iterator;
        assertEquals(90000, casted.cardinality(Integer.MAX_VALUE));
        log.info("Expected 90000, got {}.", casted.cardinality(200000));

        NodeId valid = backend.getId("<http://purl.org/goodrelations/validThrough>");
        it = backend.search(backend.any(), valid, backend.any());
        casted = (ProgressJenaIterator) ((LazyIterator) it).iterator;
        assertEquals(36346, casted.cardinality(Integer.MAX_VALUE));
        log.info("Expected 36346, got {}.", casted.cardinality(200000));

        NodeId region = backend.getId("<http://schema.org/eligibleRegion>");
        NodeId country21 = backend.getId("<http://db.uwaterloo.ca/~galuc/wsdbm/Country21>");
        it = backend.search(backend.any(), region, country21);
        casted = (ProgressJenaIterator) ((LazyIterator) it).iterator;
        assertEquals(2613, casted.cardinality(Integer.MAX_VALUE));
        log.info("Expected 2613, got {}.", casted.cardinality(200000));
    }

    @Test
    public void count_as_well_but_building_a_tree_in_a_bplustree_way() {
        JenaBackend backend = new JenaBackend("../target/watdiv10M");

        NodeId price = backend.getId("<http://purl.org/goodrelations/price>");
        var it = backend.search(backend.any(), price, backend.any());
        ProgressJenaIterator casted = (ProgressJenaIterator) ((LazyIterator) it).iterator;
        assertEquals(240000, casted.getTreeOfCardinality().sum);

        NodeId eligible = backend.getId("<http://schema.org/eligibleQuantity>");
        it = backend.search(backend.any(), eligible,backend.any());
        casted =(ProgressJenaIterator)((LazyIterator)it).iterator;
        assertEquals(90000, casted.getTreeOfCardinality().sum);

        NodeId text = backend.getId("<http://schema.org/text>");
        it = backend.search(backend.any(),text,backend.any());
        casted =(ProgressJenaIterator)((LazyIterator)it).iterator;
        assertEquals(7476, casted.getTreeOfCardinality().sum);

        NodeId include = backend.getId("<http://purl.org/goodrelations/includes>");
        it = backend.search(backend.any(),include,backend.any());
        casted =(ProgressJenaIterator)((LazyIterator)it).iterator;
        assertEquals(90000, casted.getTreeOfCardinality().sum);

        NodeId valid = backend.getId("<http://purl.org/goodrelations/validThrough>");
        it = backend.search(backend.any(),valid,backend.any());
        casted =(ProgressJenaIterator)((LazyIterator)it).iterator;
        assertEquals(36346, casted.getTreeOfCardinality().sum);

        NodeId region = backend.getId("<http://schema.org/eligibleRegion>");
        NodeId country21 = backend.getId("<http://db.uwaterloo.ca/~galuc/wsdbm/Country21>");
        it = backend.search(backend.any(),region,country21);
        casted =(ProgressJenaIterator)((LazyIterator)it).iterator;
        assertEquals(2613, casted.getTreeOfCardinality().sum);
    }

    @Disabled
    @Test
    public void getting_the_distribution_of_watdiv_spo_in_balanced_tree_index() {
        ProgressJenaIterator.NB_WALKS = 1000;

        JenaBackend backend = new JenaBackend("../target/watdiv10M");
        ProgressJenaIterator it = (ProgressJenaIterator) ((LazyIterator) backend.search(backend.any(), backend.any(), backend.any())).iterator;
        HashMap<Record, ImmutableTriple<Double, Double, Double>> recordToProba = new HashMap<>();
        log.debug("Start random sampling…");
        for (int i = 0; i < 100_000; ++i) {
            var rWp = it.randomWithProbability();
            Tuple<NodeId> ids = backend.getId(rWp.getLeft());
            LazyIterator s = (LazyIterator) backend.search(ids.get(0), backend.any(), backend.any());
            ProgressJenaIterator sR = (ProgressJenaIterator) s.iterator;
            LazyIterator o = (LazyIterator) backend.search(backend.any(), backend.any(), ids.get(2));
            ProgressJenaIterator oR = (ProgressJenaIterator) o.iterator;
            recordToProba.put(rWp.getLeft(), new ImmutableTriple<>(rWp.getRight(), oR.cardinality(), sR.cardinality()));
        }

        var sortedProbas = recordToProba.values().stream().sorted(Comparator.comparing(a -> a.left)).collect(Collectors.toList());

        List<ImmutableTriple<Double, Double, Double>> accepted = new ArrayList<>();

        for (ImmutableTriple<Double, Double, Double> triple : sortedProbas) {
            double randomNumber= new Random().nextDouble();
            if (randomNumber <= (1./it.cardinality())/triple.getLeft()) { // p = getLeft * 1/|N|/getLeft = 1/|N|
                accepted.add(triple);
            }
        }

        System.out.println("Size of unique triples = " + sortedProbas.size());
        System.out.println("Size of accepted = " + accepted.size());
        // normalized.forEach(p -> System.out.println(String.format("%s %s %s", p.getLeft(), p.getMiddle(), p.getRight())));

        double sum = 0.;
        for (var triple : accepted) {
            sum += 1. / triple.getRight(); // middle : object ; right : subject
        }
        double estimate = it.cardinality() / accepted.size() * sum;
        System.out.println(estimate);
    }


    @Disabled
    @Test
    public void testing_for_count_distinct() {
        ProgressJenaIterator.NB_WALKS = 1000;
        JenaBackend backend = new JenaBackend("../target/watdiv10M");
        LazyIterator spo = (LazyIterator) backend.search(backend.any(), backend.any(), backend.any());
        ProgressJenaIterator spoR = (ProgressJenaIterator) spo.iterator;
        double sampleSize = 10000.;

        for (int j = 0; j < 5; ++j) {
            double sum = 0.;
            for (int i = 0; i < sampleSize; ++i) {
                var rWp = spoR.randomWithProbability();
                Tuple<NodeId> ids = backend.getId(rWp.getLeft());
                LazyIterator o = (LazyIterator) backend.search(backend.any(), backend.any(), ids.get(2));
                //LazyIterator o = (LazyIterator) backend.search(ids.get(0), backend.any(), backend.any());
                ProgressJenaIterator oR = (ProgressJenaIterator) o.iterator;
                sum += 1. / oR.cardinality();
            }
            double estimate = spoR.cardinality() / sampleSize * sum;
            System.out.println(estimate);
        }
    }


    @Disabled
    @Test
    public void try_to_get_a_uniform_sample_out_of_spo() {
        ProgressJenaIterator.NB_WALKS = 1000;
        JenaBackend backend = new JenaBackend("../target/watdiv10M");
        LazyIterator spo = (LazyIterator) backend.search(backend.any(), backend.any(), backend.any());
        ProgressJenaIterator spoR = (ProgressJenaIterator) spo.iterator;

        int sampleSize = 10_000;
        List<Double> results = new ArrayList<>();
        for (int j = 0; j < 10; ++j) {
            double sum = 0.;
            for (int i = 0; i < sampleSize; ++i) {
                var rWp = spoR.getUniformRandom();
                Tuple<NodeId> ids = backend.getId(rWp);
                // LazyIterator o = (LazyIterator) backend.search(backend.any(), backend.any(), ids.get(2));
                LazyIterator o = (LazyIterator) backend.search(ids.get(0), backend.any(), backend.any());
                ProgressJenaIterator oR = (ProgressJenaIterator) o.iterator;
                sum += 1. / oR.cardinality();
            }
            double estimate = spoR.cardinality() / sampleSize * sum;
            System.out.println(estimate);
            results.add(estimate);
        }

        Double average = results.stream().mapToDouble(v->v).average().getAsDouble();
        System.out.println("Average = " + average);
        System.out.println("Error = " + Math.abs(average-521_585)/521_585);
        //System.out.println("Error = " + Math.abs(average-1_005_832)/1_005_832);

    }

    @Disabled
    @Test
    public void try_resampling_using_counts() {
        ProgressJenaIterator.NB_WALKS = 1000;

        JenaBackend backend = new JenaBackend("../target/watdiv10M");
        ProgressJenaIterator it = (ProgressJenaIterator) ((LazyIterator) backend.search(backend.any(), backend.any(), backend.any())).iterator;
        // Map<Record, ImmutableTriple<Double, Double, Double>> recordToProba = new HashMap<>();
        Map<NodeId, Double> object2card = new HashMap<>();
        Map<NodeId, Double> subject2card = new HashMap<>();
        Map<NodeId, Double> subject2proba = new HashMap<>();
        List<ImmutablePair<Double, Double>> sampleWithProbaAndCard = new ArrayList<>();
        log.debug("Start random sampling…");
        Double SAMPLESIZE = 100_000.;
        for (int i = 0; i < SAMPLESIZE; ++i) {
            // var rWp = new ImmutablePair<>(it.getUniformRandom(), 1./ it.getTreeOfCardinality().sum);
            var rWp = it.randomWithProbability();
            // Tuple<NodeId> ids = backend.getId(rWp.getLeft());
            Tuple<NodeId> ids = backend.getId(rWp.getLeft());
            LazyIterator s = (LazyIterator) backend.search(ids.get(0), backend.any(), backend.any());
            ProgressJenaIterator sR = (ProgressJenaIterator) s.iterator;
            // LazyIterator o = (LazyIterator) backend.search(backend.any(), backend.any(), ids.get(2));
            // ProgressJenaIterator oR = (ProgressJenaIterator) o.iterator;
            // recordToProba.put(rWp.getLeft(), new ImmutableTriple<>(rWp.getRight(), oR.cardinality(), sR.cardinality()));
            // subject2card.put(ids.get(0), sR.cardinality());
            // subject2proba.put(ids.get(0), rWp.getRight());
            // object2card.put(ids.get(2), oR.cardinality());
            sampleWithProbaAndCard.add(new ImmutablePair<>(rWp.getRight(), (double) sR.count()));
        }

        double sumOfCards = subject2card.values().stream().mapToDouble(v->v).sum();
        List<Double> subjectCards = subject2card.values().stream().toList();
        log.debug("Resampling…");
        var rng = new Random();
        List<Double> resample = new ArrayList<>();
        /*for (int j = 0; j < 1_000_000; ++j) {
            int random = rng.nextInt((int) Math.ceil(sumOfCards));
            double currentSum = 0.;
            int i = 0;
            while (currentSum <= random && i < subjectCards.size()) {
                currentSum += subjectCards.get(i);
                ++i;
            }
            i = i-1;
            // System.out.println(i);
            double toAdd = subjectCards.get(i);
            resample.add(toAdd);
        }*/

        Double sumOfProbas = 0.;
        /*
        for (Double sampled_card: resample) {
            sumOfProbas += 1. / sampled_card;
        }

        double estimate = it.cardinality() / resample.size() * sumOfProbas;
        System.out.println("estimate = " + estimate);
        */

        // Another strategy
        /* resample = new ArrayList<>();
        for (int j = 0; j < 100_000_00; ++j) {
            int random = rng.nextInt((int) Math.ceil(subjectCards.size()));
            resample.add(subjectCards.get(random));
        }

        sumOfProbas = 0.;
        for (Double sampled_card: resample) {
            sumOfProbas += 1./sampled_card;

        }
        double estimate = it.cardinality() / resample.size() * sumOfProbas;
        System.out.println("estimate = " + estimate);

           */

        // Another strategy
        Double maxProba = sampleWithProbaAndCard.stream().mapToDouble(ImmutablePair::getLeft).max().getAsDouble();
        List<ImmutablePair<Double,Double>> subjectProbas = sampleWithProbaAndCard.stream().map(e->
                new ImmutablePair<>(1./e.getLeft() * maxProba, e.getRight()))
                .collect(Collectors.toList());
        sumOfCards = subjectProbas.stream().mapToDouble(ImmutablePair::getLeft).sum();

        /*log.debug("Resampling…");
        rng = new Random();
        resample = new ArrayList<>();
        for (int j = 0; j < 2*SAMPLESIZE; ++j) {
            // System.out.println(j);
            double random = rng.nextDouble(sumOfCards);
            // System.out.println("random " + random);
            double currentSum = 0.;
            int i = 0;
            while (currentSum <= random && i < subjectProbas.size()) {
                currentSum += subjectProbas.get(i).getLeft();
                ++i;
            }
            i = i-1;
            // System.out.println(i);
            double toAdd = subjectProbas.get(i).getRight();
            resample.add(toAdd);
        }

        sumOfProbas = 0.;
        for (Double sampled_card: resample) {
            sumOfProbas += 1./sampled_card;
        }
        double estimate = it.count() / resample.size() * sumOfProbas;
        System.out.println("estimate = " + estimate);
        double relativeError = Math.abs(521585. - estimate)/521585.;
        System.out.println("relative error = " + relativeError);*/


        ////

        sumOfProbas = 0.;
        Double maxProba2 = sampleWithProbaAndCard.stream().mapToDouble(ImmutablePair::getLeft).max().getAsDouble();
        Double sumOfRevisedSample = 0.;
        for (ImmutablePair<Double, Double> result : sampleWithProbaAndCard) {
            sumOfProbas += (1./result.getLeft())*maxProba2/result.getRight();
            sumOfRevisedSample += 1./result.getLeft()*maxProba2;
        }

        double estimate = (it.count() / sumOfRevisedSample) * sumOfProbas;
        double relativeError = Math.abs(521585. - estimate)/521585.;
    }


    @Disabled
    @Test
    public void check_uniformity_of_triple_pattern() {
        ProgressJenaIterator.NB_WALKS = 1000;

        int DISTINCT = 15000;
        ArtificallySkewedGraph graph = new ArtificallySkewedGraph(DISTINCT, 50);
        JenaBackend backend = new JenaBackend(graph.getDataset());
        int SAMPLESIZE = 5000000;
        NodeId is_a = backend.getId("<http://is_a>", SPOC.PREDICATE);
        NodeId prof = backend.getId("<http://Prof>", SPOC.OBJECT);
        NodeId teaches = backend.getId("<http://teaches>", SPOC.PREDICATE);
        NodeId belongs_to = backend.getId("<http://belongs_to>", SPOC.PREDICATE);

        ////////////

        ProgressJenaIterator pIsAProf = (ProgressJenaIterator)((LazyIterator) backend.search(backend.any(), is_a, prof)).iterator;
        Map<Record, Integer> record2Count = new HashMap<>();
        for (int i = 0; i < SAMPLESIZE; ++i) {
            Record record = pIsAProf.getUniformRandom();
            if (!record2Count.containsKey(record)){
                record2Count.put(record, 0);
            }
            record2Count.put(record, record2Count.get(record) + 1);
        }
        Double mean = record2Count.values().stream().mapToDouble(v->v).average().getAsDouble();
        Double max  =record2Count.values().stream().mapToDouble(v->v).max().getAsDouble();
        Double min = record2Count.values().stream().mapToDouble(v->v).min().getAsDouble();
    }

    @Disabled
    @Test
    public void performing_skewed_query() {
        ProgressJenaIterator.NB_WALKS = 1000;

        int DISTINCT = 10000;
        ArtificallySkewedGraph graph = new ArtificallySkewedGraph(DISTINCT, 50);
        JenaBackend backend = new JenaBackend(graph.getDataset());


        ProgressJenaIterator spo = (ProgressJenaIterator)((LazyIterator) backend.search(backend.any(), backend.any(), backend.any())).iterator;
        Long cardinality = spo.count();
        Double estimated = spo.cardinality();
        Double relativeErr = Math.abs(cardinality- estimated)/cardinality;

        ///////////////////

        int SAMPLESIZE = 20000;
        NodeId is_a = backend.getId("<http://is_a>", SPOC.PREDICATE);
        NodeId prof = backend.getId("<http://Prof>", SPOC.OBJECT);
        NodeId teaches = backend.getId("<http://teaches>", SPOC.PREDICATE);
        NodeId belongs_to = backend.getId("<http://belongs_to>", SPOC.PREDICATE);

        //////////// run the full query

        Double total = 0.;
        BackendIterator<NodeId, ?> pIsAProfIt = backend.search(backend.any(), is_a, prof);
        Map<NodeId, Double> group2cardinality = new HashMap<>();
        while (pIsAProfIt.hasNext()) {
            pIsAProfIt.next();
            // ?p teaches ?s
            BackendIterator<NodeId, ?> pTeachesSIt = backend.search(pIsAProfIt.getId(SPOC.SUBJECT), teaches, backend.any());
            while (pTeachesSIt.hasNext()) {
                pTeachesSIt.next();
                BackendIterator<NodeId, ?> sBelongsToGIt = backend.search(pTeachesSIt.getId(SPOC.OBJECT), belongs_to, backend.any());

                boolean found = false;
                while (sBelongsToGIt.hasNext()){
                    found = true;
                    sBelongsToGIt.next();
                //    System.out.println(total);
                    total += 1;
                    if (!group2cardinality.containsKey(sBelongsToGIt.getId(SPOC.OBJECT))) {
                        group2cardinality.put(sBelongsToGIt.getId(SPOC.OBJECT), 0.);
                    }
                    group2cardinality.put(sBelongsToGIt.getId(SPOC.OBJECT), group2cardinality.get(sBelongsToGIt.getId(SPOC.OBJECT)) + 1);
                }
                if (!found) { // failures get recorded in "any"
                    if (!group2cardinality.containsKey(backend.any())) {
                        group2cardinality.put(backend.any(), 0.);
                    }
                    group2cardinality.put(backend.any(), group2cardinality.get(backend.any()) + 1);
                }
            }
        }

        //////// Draw a sample

        Double actualSampleSize = 0.;
        Double sum = 0.;
        List<ImmutableTriple<Double, String, Double>> resultsProbaAndCard = new ArrayList<>();
        Map<String, Double> id2count = new HashMap<>();
        // ?p is_a Prof
        ProgressJenaIterator pIsAProf = (ProgressJenaIterator)((LazyIterator) backend.search(backend.any(), is_a, prof)).iterator;
        for (int i = 0; i < SAMPLESIZE; ++i) {

            Record pRecord = pIsAProf.getUniformRandom();
            Double firstTripleProba = 1./pIsAProf.count();
            // var pRP = pIsAProf.randomWithProbability();
            // Record pRecord = pRP.getLeft();
            // Double firstTripleProba = pRP.getRight();
            NodeId pId = backend.getId(pRecord).get(SPOC.OBJECT); // TODO id of Record reordered depending on used index

            String id = pRecord.toString();

            // ?p teaches ?s
            ProgressJenaIterator pTeachesS = (ProgressJenaIterator)((LazyIterator) backend.search(pId, teaches, backend.any())).iterator;
            Record sRecord = pTeachesS.getUniformRandom();
            Double secondTripleProba = 1./ pTeachesS.count();
            // var sRP = pIsAProf.randomWithProbability();
            // Record sRecord = sRP.getLeft();
            // Double secondTripleProba = sRP.getRight();

            NodeId sId = backend.getId(sRecord).get(SPOC.OBJECT);

            id += sRecord;

            ProgressJenaIterator sBelongsToG = (ProgressJenaIterator)((LazyIterator) backend.search(sId, belongs_to, backend.any())).iterator;

            if (sBelongsToG.count() >= 1) { // TODO ugly but needed for this
                Record gRecord = sBelongsToG.getUniformRandom();
                NodeId gId = backend.getId(gRecord).get(SPOC.OBJECT);
                id += gRecord;

                // String pString = backend.getValue(gId);
                // System.out.println(pString);
                // here group are unique each teacher, so its easier, but we should count in the global query
                sum += 1. / group2cardinality.get(gId); //sBelongsToG.count(); // which is always 1 actually
                actualSampleSize += 1.;
                resultsProbaAndCard.add(new ImmutableTriple<>(
                        firstTripleProba * secondTripleProba * 1. / 1.,
                        id,
                        group2cardinality.get(gId)
                ));

                if (!id2count.containsKey(id))
                    id2count.put(id, 0.);

                id2count.put(id, id2count.get(id) + 1);
            }
        }

        Map<Double, Integer> distributionOfCardinality = new HashMap<>();
        for (ImmutableTriple<Double, String, Double> p : resultsProbaAndCard) {
            if (!distributionOfCardinality.containsKey(p.getRight())) {
                distributionOfCardinality.put(p.getRight(), 0);
            }
            distributionOfCardinality.put(p.getRight(), distributionOfCardinality.get(p.getRight())+ 1);
        }
        List<Integer> sorted = distributionOfCardinality.values().stream().sorted().toList();

        double estimatedCountDistinct = total / actualSampleSize * sum;
        double relativeErrCountDistinct = Math.abs(DISTINCT-estimatedCountDistinct)/DISTINCT;

        ///////////////////////////////////////////

        Double maxProba = resultsProbaAndCard.stream().mapToDouble(ImmutableTriple::getLeft).max().getAsDouble();
        resultsProbaAndCard = resultsProbaAndCard.stream().map(e ->
                new ImmutableTriple<Double, String, Double>(1./e.getLeft() * maxProba,// * 1./id2count.get(e.getMiddle()),
                        e.getMiddle(),
                        e.getRight())
        ).collect(Collectors.toList());

        log.debug("Resampling…");
        double sumOfProbas = resultsProbaAndCard.stream().mapToDouble(ImmutableTriple::getLeft).sum();
        Random rng = new Random();
        List<Double> resample = new ArrayList<>();
        for (int j = 0; j < 2*SAMPLESIZE; ++j) { // TODO size of resample?
            // System.out.println(j);
            double random = rng.nextDouble(sumOfProbas);
            // System.out.println("random " + random);
            double currentSum = 0.;
            int i = 0;
            while (currentSum <= random && i < resultsProbaAndCard.size()) {
                currentSum += resultsProbaAndCard.get(i).getLeft();
                ++i;
            }
            i = i-1;
            // System.out.println(i);
            double toAdd = resultsProbaAndCard.get(i).getRight();
            resample.add(toAdd);
        }

        sumOfProbas = 0.;
        for (Double resampledCardinality: resample) {
                sumOfProbas += 1./resampledCardinality;
        }


        double resampleEstimate = total / resample.size() * sumOfProbas;
        double resampleRelativeErrCountDistinct = Math.abs(DISTINCT-resampleEstimate)/DISTINCT;
    }



}