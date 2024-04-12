package org.apache.kafka.storage.internals.log;

import java.util.LinkedList;
import java.util.Timer;
import java.util.TimerTask;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.EnhancedDoubleHasher;
import org.apache.commons.collections4.bloomfilter.LayerManager;
import org.apache.commons.collections4.bloomfilter.LayeredBloomFilter;
import org.apache.commons.collections4.bloomfilter.Shape;
import org.apache.commons.collections4.bloomfilter.SimpleBloomFilter;
import org.apache.commons.collections4.bloomfilter.WrappedBloomFilter;
import org.apache.commons.collections4.bloomfilter.LayerManager.Cleanup;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

public class ProducerIDQuotaManager {
    private final ConcurrentHashMap<KafkaPrincipal, PIDFilter> map;
    private final Shape shape;
    private final int layerCount;
    protected final AtomicLong ttl;
    protected final Timer timer;
    
    
    public ProducerIDQuotaManager(Shape shape, long ttl, int windowCount) {
        this.map = new ConcurrentHashMap<>();
        this.shape = shape;
        this.ttl = new AtomicLong(ttl);
        this.layerCount = windowCount;
        this.timer = new Timer();
        
        timer.schedule(new TimerTask() { public void run() { cleanMap();}}, ttl, ttl);
    }
    
    void framingSizeInMs(long ttl) {
        this.ttl.set(ttl);
    }
    
    public boolean containsKeyValuePair(KafkaPrincipal principal, long pid) {
        
        PIDFilter pidFilter = map.get(principal);
        return pidFilter == null ? false : pidFilter.contains(makeFilter(pid));
    }
    
    public void add(KafkaPrincipal principal, long pid) {
        BloomFilter bf = makeFilter(pid);
        BiFunction<KafkaPrincipal, PIDFilter, PIDFilter>  adder = (k,f) -> f == null ? new PIDFilter(bf) : f.merge(bf);
        map.compute(principal, adder);
    }

    BloomFilter makeFilter(long pid) {
        long[] split = { pid & 0xFFFFFFFFL, pid & 0xFFFFFFFF00000000L }; 
        EnhancedDoubleHasher hasher = new EnhancedDoubleHasher(split[0], split[1]);
        BloomFilter result = new SimpleBloomFilter(shape);
        result.merge(hasher);
        return result;
    }

    void cleanMap() {
        map.values().removeIf( PIDFilter::isEmpty);
    }

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
    
    public class PIDFilter {
        private LayeredBloomFilter bloomFilter;
        private LayerManager layerManager; 
    
        public PIDFilter() {
            Predicate<LayerManager> advance = LayerManager.ExtendCheck.advanceOnCount((int)shape.estimateMaxN()).or(new TimerPredicate());
            
            layerManager = LayerManager.builder().setCleanup(cleanup).setSupplier( ()-> new TimedBloomFilter(shape, ttl.get()))
                    .setExtendCheck(advance).build();
            
            bloomFilter = new LayeredBloomFilter(shape, layerManager);
        }
        
        public PIDFilter(BloomFilter bf) {
            this();
            bloomFilter.merge(bf);
        }
        
        PIDFilter merge(BloomFilter bf) {
            bloomFilter.merge(bf);
            return this;
        }
        
        boolean contains(BloomFilter bf) {
            return bloomFilter.contains(bf);
        }
        
        boolean isEmpty() {
            bloomFilter.next(); // forces clean
            return bloomFilter.isEmpty();
        }
        
        class TimerPredicate implements Predicate<LayerManager> {
            long expires;
 
            
            TimerPredicate() {
                expires = System.currentTimeMillis() + (ttl.get()/layerCount);
            }
            
            @Override
            public boolean test(LayerManager o) {
                long now = System.currentTimeMillis();
                if (expires > now) {
                    return false;
                }
                expires = (ttl.get()/layerCount);
                return true;
            }
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