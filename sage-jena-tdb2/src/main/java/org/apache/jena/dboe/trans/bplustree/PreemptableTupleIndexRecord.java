package org.apache.jena.dboe.trans.bplustree;

import fr.gdd.sage.ReflectionUtils;
import org.apache.jena.atlas.iterator.SingletonIterator;
import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.atlas.lib.tuple.TupleFactory;
import org.apache.jena.atlas.lib.tuple.TupleMap;
import org.apache.jena.dboe.base.record.Record;
import org.apache.jena.dboe.base.record.RecordFactory;
import org.apache.jena.dboe.base.record.RecordMapper;
import org.apache.jena.dboe.index.RangeIndex;
import org.apache.jena.tdb2.store.NodeId;
import org.apache.jena.tdb2.store.NodeIdFactory;
import org.apache.jena.tdb2.store.tupletable.TupleIndexBase;
import org.apache.jena.tdb2.store.tupletable.TupleIndexRecord;
import org.apache.jena.util.iterator.NullIterator;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static org.apache.jena.tdb2.sys.SystemTDB.SizeOfNodeId;


public class PreemptableTupleIndexRecord {

    RecordFactory factory;
    RangeIndex index;
    TupleMap tupleMap;
    
    public TupleIndexRecord tir;

    private final RecordMapper<Tuple<NodeId>> recordMapper;


    
    public PreemptableTupleIndexRecord(TupleIndexRecord tir) {
        Field factoryField = ReflectionUtils._getField(TupleIndexRecord.class, "factory");
        this.factory = (RecordFactory) ReflectionUtils._callField(factoryField, tir.getClass(), tir);
        Field tupleMapField = ReflectionUtils._getField(TupleIndexBase.class, "tupleMap");
        this.tupleMap = (TupleMap) ReflectionUtils._callField(tupleMapField, TupleIndexBase.class, tir);

        final int keyLen = factory.keyLength();
        final int numNodeIds = factory.keyLength() / NodeId.SIZE;

        this.index = tir.getRangeIndex(); 
        this.tir = tir;

        recordMapper = (bb, entryIdx, key, recFactory) -> {
            // Version one. (Skipped) : index-order Tuple<NodeId>, then remap.
            // Version two.
            //   Straight to right order.
            // Version three
            //   Straight to right order. Delay creation.

            int bbStart = entryIdx*recFactory.recordLength();
            // Extract the bytes, index order for the key test..
            if ( key != null ) {
                bb.position(bbStart);
                bb.get(key, 0, keyLen);
            }

            // Now directly create NodeIds, no Record.
            NodeId[] nodeIds = new NodeId[numNodeIds];
            for ( int i = 0; i < numNodeIds ; i++ ) {
                int j = i;
                if ( tupleMap != null )
                    j = tupleMap.unmapIdx(i);
                // Get data. It is faster to get from the ByteBuffer than from the key byte[].
                NodeId id = NodeIdFactory.get(bb, bbStart+j*NodeId.SIZE);
                nodeIds[i] = id;
            }
            return TupleFactory.create(nodeIds);
        };
    }

    public BetterJenaIterator scan(Tuple<NodeId> patternNaturalOrder) {
        Tuple<NodeId> pattern = tupleMap.map(patternNaturalOrder);
        
        // Canonical form.
        int numSlots = 0;
        int leadingIdx = -2;  // Index of last leading pattern NodeId.  Start less than numSlots-1
        boolean leading = true;

        // Records.
        Record minRec = factory.createKeyOnly();
        Record maxRec = factory.createKeyOnly();

        // Set the prefixes.
        for ( int i = 0; i < pattern.len() ; i++ ) {
            NodeId X = pattern.get(i);
            if ( NodeId.isAny(X) ) {
                X = null;
                // No longer seting leading key slots.
                leading = false;
                continue;
            }
            // if ( NodeId.isDoesNotExist(X) )
            //     return new BetterJenaIterator();

            numSlots++;
            if ( leading ) {
                leadingIdx = i;
                NodeIdFactory.set(X, minRec.getKey(), i*SizeOfNodeId);
                NodeIdFactory.set(X, maxRec.getKey(), i*SizeOfNodeId);
            }
        }

        // Is it a simple existence test?
        if ( numSlots == pattern.len() ) {
             if ( index.contains(minRec) ) {
                 return new BetterJenaIterator(new SingletonIterator<>(pattern));
             } else {
                 return new BetterJenaIterator(new NullIterator<>());
             }
         }
        
        // Iterator<Tuple<NodeId>> tuples;
        BetterJenaIterator tuples;
        if ( leadingIdx < 0 ) {
            // fullScan always allowed
            // if ( ! fullScanAllowed )
            // return null;
            // Full scan necessary
            RangeIndex rIndex = tir.getRangeIndex();
            BPlusTree bpt = (BPlusTree) rIndex;
            tuples = new BetterJenaIterator(bpt, null, null, recordMapper, factory, tupleMap);
        } else {
            // Adjust the maxRec.
            NodeId X = pattern.get(leadingIdx);
            // Set the max Record to the leading NodeIds, +1.
            // Example, SP? inclusive to S(P+1)? exclusive where ? is zero.
            NodeIdFactory.setNext(X, maxRec.getKey(), leadingIdx*SizeOfNodeId);

            // tuples = index.iterator(minRec, maxRec, recordMapper);

            RangeIndex rIndex = tir.getRangeIndex();
            BPlusTree bpt = (BPlusTree) rIndex;

            tuples = new BetterJenaIterator(bpt, minRec, maxRec, recordMapper, factory, tupleMap);
        }
        
        if ( leadingIdx < numSlots-1 ) {
            // partial scan always allowed
            // if ( ! partialScanAllowed )
            // return null;
            // Didn't match all defined slots in request.
            // Partial or full scan needed.
            //pattern.unmap(colMap);
            Method scanMethod = ReflectionUtils._getMethod(TupleIndexRecord.class, "scan");
            // tuples = scan(tuples, patternNaturalOrder);
            // tuples = (Iterator<Tuple<NodeId>>) ReflectionUtils._callMethod(scanMethod, tir.getClass(), tir,
            // tuples, patternNaturalOrder);
            // (TODO)
            tuples = null;
        }
        
        return tuples;
        
    }

}
