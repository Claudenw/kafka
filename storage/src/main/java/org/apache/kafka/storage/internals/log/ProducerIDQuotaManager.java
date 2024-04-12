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

/**
 * ProducerIDQuotaManager as descriped in KIP-936
 *
 */
public class ProducerIDQuotaManager {
    /** the bloom principal to Bloom filter map */
    private final ConcurrentHashMap<KafkaPrincipal, PIDFilter> map;
    /** The shape of the Bloom filters */
    private final Shape shape;
    /** The number of expected layers */
    private final int layerCount;
    /** The time to live for a single layer (e.g. 1 hour) in milliseconds */
    protected final AtomicLong ttl;
    /** A timer for misc cleanup */
    protected final Timer timer;
    
    /**
     * Constructor.  This manager tracks the PIDs for each principal.
     * PIDs will expire from the Manager after the number of milliseconds specified in the {@code ttl}
     * parameter.  A new window is started every ttl/windowCount milliseconds.
     * @param shape The shape for the bloom filters.  This describes number of items and expected false positive rates.
     * @param ttl The number of milliseconds each window should last.
     * @param windowCount  The number of windows that should be active at any one time.
     */
    public ProducerIDQuotaManager(Shape shape, long ttl, int windowCount) {
        this.map = new ConcurrentHashMap<>();
        this.shape = shape;
        this.ttl = new AtomicLong(ttl);
        this.layerCount = windowCount;
        this.timer = new Timer();
        // run the cleanup as windows expire.
        timer.schedule(new TimerTask() { public void run() { cleanMap();}}, ttl, ttl);
    }
    
    /**
     * As defined in KIP-936 This is how long a window should survive.
     * @param ttl the milliseconds that a window should survive.
     */
    void framingSizeInMs(long ttl) {
        this.ttl.set(ttl);
    }
    
    /**
     * Checks for the presence of the princpaal/pid key value pair.
     * @param principal the principal to searchfor.
     * @param pid the PID to search for.
     * @return true if the pair are found.
     */
    public boolean containsKeyValuePair(KafkaPrincipal principal, long pid) {
        PIDFilter pidFilter = map.get(principal);
        return pidFilter == null ? false : pidFilter.contains(makeFilter(pid));
    }

    /**
     * Add the principla/pid pair to the manager.
     * @param principal the principal to add
     * @param pid the pair associated with the principal
     */
    public void add(KafkaPrincipal principal, long pid) {
        BloomFilter bf = makeFilter(pid);
        BiFunction<KafkaPrincipal, PIDFilter, PIDFilter>  adder = (k,f) -> f == null ? new PIDFilter(bf) : f.merge(bf);
        map.compute(principal, adder);
    }

    /**
     * Creates a Bloom filter for the pid.
     * @param pid the pid to create the Bloom filter for.
     * @return the Bloomfilter.
     */
    BloomFilter makeFilter(long pid) {
        /*
         * This is a fairly naive split of the pid into a buffer by placing all odd nibbles in the initial value and all the 
         * even nibbles in the increment value of the EnhancedDoubleHasher.
         * and all odd nibbles in the other.
         */
        EnhancedDoubleHasher hasher = new EnhancedDoubleHasher(pid & 0x0f0f0f0f0f0f0f0fL, pid & 0xf0f0f0f0f0f0f0f0L);
        BloomFilter result = new SimpleBloomFilter(shape);
        result.merge(hasher);
        return result;
    }

    /**
     * Cleans any principals that have not had any input in during the last window timeout from the manager.
     */
    void cleanMap() {
        map.values().removeIf(PIDFilter::isEmpty);
    }

    /* 
     * Removes any filter that is empty or has expired.
     */
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
    /**
     * The PID based Filter.  Utilizes a LayeredBloomFilter to track the PIDS created in the last window.
     *
     */
    public class PIDFilter {
        // the layered Bloom filter.
        private LayeredBloomFilter bloomFilter;
        // the layer manager for the Bloom filter.
        private LayerManager layerManager; 
    
        public PIDFilter() {
            // we will create a new Bloom filter every time the active filter in the layered filter becomes full or when the next window 
            // should be started.
            Predicate<LayerManager> advance = LayerManager.ExtendCheck.advanceOnCount((int)shape.estimateMaxN()).or(new TimerPredicate());
            // the manager for the layer.  Performs automatic cleanup and advance when necessary.
            layerManager = LayerManager.builder().setCleanup(cleanup).setSupplier( ()-> new TimedBloomFilter(shape, ttl.get()))
                    .setExtendCheck(advance).build();
            // the layered bloom filter.
            bloomFilter = new LayeredBloomFilter(shape, layerManager);
        }
        
        /**
         * Constructor for a PIDFilter that has a starting Bloom filter.
         * @param bf
         */
        public PIDFilter(BloomFilter bf) {
            this();
            bloomFilter.merge(bf);
        }

        // merge a Bloom filter into the layered bloom filter.
        PIDFilter merge(BloomFilter bf) {
            bloomFilter.merge(bf);
            return this;
        }

        /**
         * Returns true if {@code bf} is found in the layered filter.
         * @param bf the filter to look for.
         * @return true if found.
         */
        boolean contains(BloomFilter bf) {
            return bloomFilter.contains(bf);
        }

        /** 
         * @return true if there are no PIDs being tracked for the principal.
         */
        boolean isEmpty() {
            bloomFilter.next(); // forces clean
            return bloomFilter.isEmpty();
        }

        /** Determines if the system should create a new layer */
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

    /** A Bloom filter implementation that has a timestamp indicating when it expires.  
     * Used as the Bloom filter for the layer in the LayeredBloomFilter 
     */
    class TimedBloomFilter extends WrappedBloomFilter {
        long expires;
        
        TimedBloomFilter(Shape shape, long ttl) {
            super(new SimpleBloomFilter(shape));
            expires = System.currentTimeMillis() + ttl;
        }
    }
}