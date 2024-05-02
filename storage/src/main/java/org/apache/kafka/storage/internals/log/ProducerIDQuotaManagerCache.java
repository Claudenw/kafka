package org.apache.kafka.storage.internals.log;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.apache.commons.collections4.bloomfilter.*;
import org.apache.commons.collections4.bloomfilter.LayerManager.Cleanup;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

/**
 * ProducerIDQuotaManager as descriped in KIP-936
 *
 */
public class ProducerIDQuotaManagerCache {
    /** the bloom principal to Bloom filter map */
    private final ConcurrentHashMap<KafkaPrincipal, PIDFilter> map;
    /** The desired false positive rate of the Bloom filters */
    private final double falsePositiveRate;
    /** The number of expected layers */
    private final int layerCount;
    /** The time to live for a single layer (e.g. 1 hour) in milliseconds */
    private final long ttl;
    /** A timer for misc cleanup */
    private final Timer timer;

    private final long deltaT3;
    private final long deltaT;

    /**
     * Constructor.  This manager tracks the PIDs for each principal.
     * PIDs will expire from the Manager after the number of milliseconds specified in the {@code ttl}
     * parameter.  A new window is started every ttl/layerCount milliseconds.
     * @param falsePositiveRate The acceptable false positive rate.
     * @param ttl The number of milliseconds each window should last.
     * @param layerCount  The number of layers that should be active at any one time.
     */
    public ProducerIDQuotaManagerCache(Double falsePositiveRate, long ttl, int layerCount) {
        this.map = new ConcurrentHashMap<>();
        this.falsePositiveRate = falsePositiveRate;
        this.ttl = ttl;
        this.layerCount = layerCount;
        this.timer = new Timer();
        this.deltaT = ttl/layerCount;
        this.deltaT3 = 3*deltaT;
        // run the cleanup as windows expire.
        timer.schedule(new TimerTask() { public void run() { cleanMap();}}, ttl, deltaT);
    }

    /**
     * Ensures that the PID is tracked as being seen in the last 1/2 of the window.
     * @param principal the principal to track.
     * @param producerIdRate the rate per hour at which PIDs are expected to be generated.
     * @param pid the PID to track.
     * @return true if it was found in the manager.
     */
    boolean track(KafkaPrincipal principal, int producerIdRate, long pid) {
        Hasher hasher = makeHasher(pid);
        int windowRate = (int) Math.round(producerIdRate * 1.0 / layerCount);
        BiFunction<KafkaPrincipal, PIDFilter, PIDFilter>  adder = null;
        PIDFilter pidFilter = map.get(principal);
        if (pidFilter != null) {
            // create the Bloom filter here to reduce processing time.
            BloomFilter bf = new SimpleBloomFilter(pidFilter.getShape());
            bf.merge(hasher);
            // create an adder that uses the Bloom filter
            adder = (k, f) -> f == null ? new PIDFilter(windowRate, bf) : f.merge(bf);
            if (pidFilter.contains(bf)) {
                // make sure it is recent
                if (!pidFilter.isRecent(bf)) {
                    map.compute(principal, adder);
                }
                return true;
            }
        } else {
            // create an adder that uses the hasher.
            adder = (k, f) -> f == null ? new PIDFilter(windowRate, hasher) : f.merge(hasher);
        }
        map.compute(principal, adder);
        return false;
    }

    /**
     * Creates a Bloom filter for the pid.
     * @param pid the pid to create the Bloom filter for.
     * @return the Bloomfilter.
     */
    Hasher makeHasher(long pid) {
        /*
         * This is a fairly naive split of the pid into a buffer by placing all odd nibbles in the initial value and all the
         * even nibbles in the increment value of the EnhancedDoubleHasher.
         * and all odd nibbles in the other.
         */
        return new EnhancedDoubleHasher(pid & 0x0f0f0f0f0f0f0f0fL, pid & 0xf0f0f0f0f0f0f0f0L);
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
            lst -> {
                if (!lst.isEmpty()) {
                    long now = System.currentTimeMillis();
                    Iterator<BloomFilter> iter = lst.iterator();
                    while (iter.hasNext()) {
                        TimestampedBloomFilter filter = (TimestampedBloomFilter) iter.next();
                        if (filter.expires > now) {
                            iter.remove();
                        } else {
                            break;
                        }
                    }
                }
            } );
    /**
     * The PID based Filter.  Utilizes a LayeredBloomFilter to track the PIDS created in the last window.
     */
    public class PIDFilter {
        // the layered Bloom filter.
        private final LayeredBloomFilter bloomFilter;
        // the layer manager for the Bloom filter.
        private final LayerManager layerManager;

        /**
         * Creates a PIDFilter
         * @param estimatedPids the estimated number of PIDs per layer.
         */
        public PIDFilter(int estimatedPids) {
            Shape shape = Shape.fromNP(estimatedPids, falsePositiveRate);
            // we will create a new Bloom filter every time the active filter in the layered filter becomes full or when the next window 
            // should be started.
            Predicate<LayerManager> advance = LayerManager.ExtendCheck.advanceOnCount(estimatedPids).or(new TimerPredicate());
            // the manager for the layer.  Performs automatic cleanup and advance when necessary.
            layerManager = LayerManager.builder().setCleanup(cleanup).setSupplier( ()-> new TimestampedBloomFilter(shape, ttl))
                    .setExtendCheck(advance).build();
            // the layered bloom filter.
            bloomFilter = new LayeredBloomFilter(shape, layerManager);
        }

        /**
         * Constructor for a PIDFilter that has a starting Bloom filter.
         * @param bf
         */
        public PIDFilter(int estimatedPids, BloomFilter bf) {
            this(estimatedPids);
            bloomFilter.merge(bf);
        }

        /**
         * Constructor for a PIDFilter that has a starting Bloom filter.
         * @param estimatedPids The estimated number of expected in a layer
         */
        public PIDFilter(int estimatedPids, Hasher hasher) {
            this(estimatedPids);
            bloomFilter.merge(hasher);
        }

        /**
         * Get the shape of the enclose Bloom filter.
         * @return the encloded Bloom filter shape.
         */
        public Shape getShape() {
            return bloomFilter.getShape();
        }

        // merge a Bloom filter into the layered bloom filter.
        PIDFilter merge(BloomFilter bf) {
            bloomFilter.merge(bf);
            return this;
        }

        PIDFilter merge(Hasher hasher) {
            bloomFilter.merge(hasher);
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
         * Returns true if {@code bf} is found in the layered filter.
         * @param hasher the hasher representation to search for.
         * @return true if found.
         */
        boolean contains(Hasher hasher) {
            return bloomFilter.contains(hasher);
        }
        /**
         * Ensures that the PID is tracked as being seen in the last 1/2 of the window.
         * @return true if it was in the last 1/2
         */
        boolean isRecent(BloomFilter bf) {
            int[] layers = bloomFilter.find(bf);
            if (layers.length > 0) {
                TimestampedBloomFilter first = (TimestampedBloomFilter) bloomFilter.get(0);
                TimestampedBloomFilter last = (TimestampedBloomFilter) bloomFilter.get(layers[layers.length-1]);
                return last.expires > first.expires+deltaT3;
            }
            return false;
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
                expires = System.currentTimeMillis() + deltaT;
            }
            
            @Override
            public boolean test(LayerManager o) {
                long now = System.currentTimeMillis();
                if (expires > now) {
                    return false;
                }
                expires = now + deltaT;
                return true;
            }
        }
    }

    /** A Bloom filter implementation that has a timestamp indicating when it expires.  
     * Used as the Bloom filter for the layer in the LayeredBloomFilter 
     */
    class TimestampedBloomFilter extends WrappedBloomFilter {
        long expires;
        
        TimestampedBloomFilter(Shape shape, long ttl) {
            super(new SimpleBloomFilter(shape));
            expires = System.currentTimeMillis() + ttl;
        }
    }
}