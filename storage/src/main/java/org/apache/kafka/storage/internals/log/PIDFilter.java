package org.apache.kafka.storage.internals.log;

import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.apache.commons.collections4.bloomfilter.*;
import org.apache.commons.collections4.bloomfilter.LayerManager.Cleanup;

public class PIDFilter {
    private LayeredBloomFilter bloomFilter;
    private final Shape shape;
    private final long ttl;

    private static Consumer<LinkedList<BloomFilter>> cleanup = Cleanup.removeEmptyTarget().andThen( 
        ll -> {
            long now = System.currentTimeMillis();
            TimedBloomFilter filter = null;
            try {
                filter = (TimedBloomFilter) ll.getFirst();
                while (filter.expires > now) {
                    ll.removeFirst();
                    filter = (TimedBloomFilter) ll.getFirst();
                }
            } catch (NoSuchElementException expected) {
               // do nothing
            }
        } );
    
    public PIDFilter(Shape shape, long ttl) {
        this.shape = shape;
        this.ttl = ttl;
        Predicate<LayerManager> advance = LayerManager.ExtendCheck.advanceOnCount((int)shape.estimateMaxN()).or(new TimerPredicate(ttl));
        
        LayerManager layerManager = LayerManager.builder().setCleanup(cleanup).setSupplier( ()-> new TimedBloomFilter(shape, ttl))
                .setExtendCheck(advance).build();
        
        bloomFilter = new LayeredBloomFilter(shape, layerManager);
    }
    
    public PIDFilter(BloomFilter bf, long ttl) {
        this(bf.getShape(), ttl);
        bloomFilter.merge(bf);
    }

    public static BloomFilter makeFilter(Shape shape, long pid) {
        long[] split = { pid & 0xFFFFFFFFL, pid & 0xFFFFFFFF00000000L }; 
        EnhancedDoubleHasher hasher = new EnhancedDoubleHasher(split[0], split[1]);
        BloomFilter result = new SimpleBloomFilter(shape);
        result.merge(hasher);
        return result;
    }
    
    boolean contains(long pid) {
        return bloomFilter.contains(makeFilter(shape, pid));
    }
    
    PIDFilter merge(BloomFilter bf) {
        bloomFilter.merge(bf);
        return this;
    }
    
    class TimerPredicate implements Predicate<LayerManager> {
        long expires;
        long ttl;
        
        TimerPredicate(long ttl) {
            this.ttl = ttl;
            expires = System.currentTimeMillis() + ttl;
        }
        
        @Override
        public boolean test(LayerManager o) {
            long now = System.currentTimeMillis();
            if (expires > now) {
                return false;
            }
            expires = now + ttl;
            return true;
        }
    }

    class TimedBloomFilter extends WrappedBloomFilter {
        long expires;
        
        TimedBloomFilter(Shape shape, long ttl) {
            super(new SimpleBloomFilter(shape));
            expires = System.currentTimeMillis() + ttl;
        }
    }
}
