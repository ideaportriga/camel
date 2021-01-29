package org.apache.camel.component.salesforce;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import mousio.etcd4j.EtcdClient;
import mousio.etcd4j.EtcdSecurityContext;
import mousio.etcd4j.requests.EtcdKeyGetRequest;
import mousio.etcd4j.requests.EtcdKeyPutRequest;
import mousio.etcd4j.responses.EtcdKeysResponse;
import org.apache.camel.RuntimeCamelException;
import org.apache.camel.api.management.ManagedOperation;
import org.apache.camel.api.management.ManagedResource;
import org.apache.camel.spi.StateRepository;
import org.apache.camel.support.service.ServiceSupport;
import org.apache.camel.util.ObjectHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This {@link EtcdReplayIdRepository} class is a ETCD-based implementation of a {@link StateRepository}.
 */
@ManagedResource(description = "ETCD based state repository")
public class EtcdReplayIdRepository extends ServiceSupport implements StateRepository<String, String> {
    private static final Logger LOG = LoggerFactory.getLogger(EtcdReplayIdRepository.class);
    private final AtomicBoolean init = new AtomicBoolean();
    private final AtomicInteger cacheCounter = new AtomicInteger();
    private static final Integer CACHE_COUNTER_INITIAL_VALUE = 0;
    private final Map<String, String> cache;
    private final SalesforceComponent component;
    private EtcdClient client;
    private Integer cacheDepth;

    public EtcdReplayIdRepository(final SalesforceComponent component) {
        this.component = component;
        this.cache = new HashMap<>();
        cacheDepth = Integer.valueOf(component.getConfig().getEtCdRepoCacheDepth());

        // del me, for testing only. With this we won't start ETCD client, making this repo in-memory repo
        if (ObjectHelper.isNotEmpty(component.getConfig().getEtcdRepoPlaceholderValue())) {
            init.set(true);
        }
    }

    // will use for tests. someday
    public EtcdReplayIdRepository(EtcdClient client, Map<String, String> cache, final SalesforceComponent component) {
        this.component = component;
        this.client = client;
        this.cache = cache;
    }

    @Override
    @ManagedOperation(description = "Adds the value of the given key to the store")
    public void setState(String key, String value) {
        synchronized (cache) {
            LOG.info("Saving in memory value {} of the given key {} to the ETCD store", value, key);
            cache.put(key, value);
            if (cacheCounter.getAndIncrement() >= cacheDepth) {
                commitReplayId(key, value);
                cacheCounter.set(CACHE_COUNTER_INITIAL_VALUE);
            }
        }
    }

    private void commitReplayId(String key, String value) {
        try {
            LOG.info("About to commit value {} of the given key {} to the ETCD store", value, key);
            EtcdKeyPutRequest request = client.put(key, value);
            request.timeout(5000, TimeUnit.MILLISECONDS);
            request.send().get();
        } catch (Exception e) {
            LOG.error("Couldn't add value {} of the given key {} to the ETCD store: {}", value, key, e.getMessage());
            throw new RuntimeCamelException(e);
        }
    }

    @Override
    @ManagedOperation(description = "Gets the value of the given key from store")
    public String getState(String key) {
        synchronized (cache) {

            populateCacheWithValueFromSettings__FOR_TESTING(key); //del me
            String valueFromCache = cache.get(key);
            if (valueFromCache != null && !valueFromCache.isEmpty()) {
                LOG.info("Got value {} of the key {} from the cache", valueFromCache, key);
                return valueFromCache;
            }

            String valueFromEtcd = null;

            try {
                EtcdKeyGetRequest request = client.get(key);
                request.timeout(5000, TimeUnit.MILLISECONDS);
                final EtcdKeysResponse response = request.send().get();
                if (Objects.nonNull(response.node)) {
                    valueFromEtcd = response.node.value;
                }
            } catch (Exception e) {
                LOG.error("Couldn't get value of the key {} from the ETCD store: {}", key, e.getMessage());
                throw new RuntimeCamelException(e);
            }

            LOG.info("Got value {} of the key {} from the ETCD store", valueFromEtcd, key);
            return valueFromEtcd;
        }
    }

    @Override
    protected void doStart() throws Exception {
        // init store if not loaded before
        if (init.compareAndSet(false, true)) {
            // create the client if it's doesn't exist
            if (client == null) {
                client = createClient(
                        component.getConfig().getEtcdRepoUri(),
                        component.getConfig().getEtcdRepoUsername(),
                        component.getConfig().getEtcdRepoPassword());
            }
            cache.clear();
            cacheCounter.set(CACHE_COUNTER_INITIAL_VALUE);
        }
    }

    public EtcdClient createClient(String uri, String username, String password) throws Exception {
        return new EtcdClient(
                // without ssl context for now
                new EtcdSecurityContext(username, password),
                resolveURIs(uri));
    }

    public static URI[] resolveURIs(String uriList) throws Exception {
        String[] uris = uriList.split(",");
        URI[] etcdUriList = new URI[uris.length];
        for (int i = 0; i < uris.length; i++) {
            etcdUriList[i] = URI.create(uris[i]);
        }
        return etcdUriList;
    }

    @Override
    protected void doStop() throws Exception {
        for (Map.Entry<String, String> entry : cache.entrySet()) {
            commitReplayId(entry.getKey(), entry.getValue());
        }

        if (client != null) {
            client.close();
            client = null;
        }

        cache.clear();
        init.set(false);
    }

    void populateCacheWithValueFromSettings__FOR_TESTING(String key) { //del me
        if (ObjectHelper.isNotEmpty(component.getConfig().getEtcdRepoPlaceholderValue())) {
            cache.put(key, component.getConfig().getEtcdRepoPlaceholderValue());
        }
    }
}
