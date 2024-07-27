/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.metadata.authorizer;


import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.errors.AuthorizerNotReadyException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.SecurityUtils;
import org.apache.kafka.metadata.authorizer.bitmap.BitMaps;
import org.apache.kafka.metadata.authorizer.trie.NameTrie;
import org.apache.kafka.metadata.authorizer.trie.Node;
import org.apache.kafka.metadata.authorizer.trie.StringMatcher;
import org.apache.kafka.metadata.authorizer.trie.Walker;
import org.apache.kafka.metadata.authorizer.trie.WildcardRegistry;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.Name;
import java.lang.reflect.Array;
import java.nio.file.attribute.AclEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.kafka.common.acl.AclOperation.ALL;
import static org.apache.kafka.common.acl.AclOperation.ALTER;
import static org.apache.kafka.common.acl.AclOperation.ALTER_CONFIGS;
import static org.apache.kafka.common.acl.AclOperation.DELETE;
import static org.apache.kafka.common.acl.AclOperation.DESCRIBE;
import static org.apache.kafka.common.acl.AclOperation.DESCRIBE_CONFIGS;
import static org.apache.kafka.common.acl.AclOperation.READ;
import static org.apache.kafka.common.acl.AclOperation.WRITE;
import static org.apache.kafka.common.acl.AclPermissionType.ALLOW;
import static org.apache.kafka.common.acl.AclPermissionType.DENY;
import static org.apache.kafka.common.resource.PatternType.LITERAL;
import static org.apache.kafka.common.resource.PatternType.PREFIXED;
import static org.apache.kafka.server.authorizer.AuthorizationResult.ALLOWED;
import static org.apache.kafka.server.authorizer.AuthorizationResult.DENIED;

/**
 * A simplified NameTrieAuthorizerData
 */
public class NameTrieAuthorizerData implements AuthorizerData {

    private static final int DESCRIBE_MAP = BitMaps.getIntBit(DESCRIBE.code()) | BitMaps.getIntBit(READ.code()) | BitMaps.getIntBit(WRITE.code()) | BitMaps.getIntBit(DELETE.code()) | BitMaps.getIntBit(ALTER.code());

    private static final int CONFIGS_MAP = BitMaps.getIntBit(DESCRIBE_CONFIGS.code()) | BitMaps.getIntBit(ALTER_CONFIGS.code());

    /**
     * The logger to use.
     */
    final Logger log;

    /**
     * Logger to use for auditing.
     */
    final Logger auditLog;

    /**
     * The current AclMutator.
     */
    final AclMutator aclMutator;

    /**
     * True if the authorizer loading process is complete.
     */
    final boolean loadingComplete;

    /**
     * A statically configured set of users that are authorized to do anything.
     */
    private final Set<String> superUsers;

    /**
     * The result to return if no ACLs match.
     */
    private final DefaultRule noAclRule;

    /**
     * Contains all of the current ACLs
     */
    private final TrieData trieData;

    public static NameTrieAuthorizerData createEmpty() {
        return new NameTrieAuthorizerData(createLogger(-1),
                null,
                false,
                Collections.emptySet(),
                DENIED,
                new TrieData());
    }

    private static Logger createLogger(int nodeId) {
        return new LogContext("[StandardAuthorizer " + nodeId + "] ").logger(StandardAuthorizerData.class);
    }

    private static Logger auditLogger() {
        return LoggerFactory.getLogger("kafka.authorizer.logger");
    }

    private NameTrieAuthorizerData(Logger log,
                                   AclMutator aclMutator,
                                   boolean loadingComplete,
                                   Set<String> superUsers,
                                   AuthorizationResult defaultResult,
                                   final TrieData trieData
                                   ) {
        this.log = log;
        this.auditLog = auditLogger();
        this.aclMutator = aclMutator;
        this.loadingComplete = loadingComplete;
        this.superUsers = superUsers;
        this.noAclRule = new DefaultRule(defaultResult);
        this.trieData = trieData;
    }

    public NameTrieAuthorizerData copyWithNewAclMutator(AclMutator newAclMutator) {
        return new NameTrieAuthorizerData(
                log,
                newAclMutator,
                loadingComplete,
                superUsers,
                noAclRule.result(),
                trieData);
    }

    public NameTrieAuthorizerData copyWithNewLoadingComplete(boolean newLoadingComplete) {
        return new NameTrieAuthorizerData(log,
                aclMutator,
                newLoadingComplete,
                superUsers,
                noAclRule.result(),
                trieData);
    }

    public NameTrieAuthorizerData copyWithNewConfig(int nodeId,
                                                    Set<String> newSuperUsers,
                                                    AuthorizationResult newDefaultResult) {
        return new NameTrieAuthorizerData(
                createLogger(nodeId),
                aclMutator,
                loadingComplete,
                newSuperUsers,
                newDefaultResult,
                trieData);
    }

    @Override
    public NameTrieAuthorizerData copyWithNewAcls(Map<Uuid, StandardAcl> acls) {
        NameTrieAuthorizerData newData =  new NameTrieAuthorizerData(
                log,
                aclMutator,
                loadingComplete,
                superUsers,
                noAclRule.result(),
                new TrieData(acls));
        log.info("Initialized with {} acl(s).", newData.aclCount());
        return newData;
    }


    public void addAcl(Uuid uuid, StandardAcl acl) {
        trieData.add(uuid, acl);
    }

    public void removeAcl(Uuid uuid) {
        trieData.remove(uuid);
    }

    @Override
    public Set<String> superUsers() {
        return superUsers;
    }

    @Override
    public AuthorizationResult defaultResult() {
        return noAclRule.result();
    }

    @Override
    public int aclCount() {
        return trieData.count();
    }

    @Override
    public AclMutator aclMutator() {
        return aclMutator;
    }

    @Override
    public Logger log() {
        return log;
    }

    @Override
    public Iterable<AclBinding> acls(AclBindingFilter filter) {
        return trieData.acls(filter);
    }

    public AuthorizationResult authorize(
            AuthorizableRequestContext requestContext,
            Action action
    ) {
        if (action.resourcePattern().patternType() != LITERAL) {
            throw new IllegalArgumentException("Only literal resources are supported. Got: " + action.resourcePattern().patternType());
        }
        KafkaPrincipal principal = AuthorizerData.baseKafkaPrincipal(requestContext);
        final MatchingRule rule;

        // Superusers are authorized to do anything.
        if (superUsers.contains(principal.toString())) {
            rule = SuperUserRule.INSTANCE;
        } else if (!loadingComplete) {
            throw new AuthorizerNotReadyException();
        } else {
            rule = findAclRule(
                    AuthorizerData.matchingPrincipals(requestContext),
                    requestContext.clientAddress().getHostAddress(),
                    action
            );
        }
        logAuditMessage(principal, requestContext, action, rule);
        return rule.result();
    }

    /**
     * Checks if the node has a matching action within the filtered ACLs.
     * @param node The node to check.
     * @param aclFilter the ACL filter to apply.
     * @param action the Action we are seeking.
     * @return an Optional StandardACL that is present if a matching ACL is found.
     */
    private Optional<StandardAcl> check(Node<NameTriePattern> node, Predicate<StandardAcl> aclFilter, Action action) {
        return node.getContents() == null ? Optional.empty() : node.getContents().first(resourcePatternFilter(node, action).and(aclFilter));
    }

    private static final Predicate<StandardAcl> literalPattern = acl -> acl.patternType() == LITERAL;
    private static final Predicate<StandardAcl> prefixedPattern = acl -> acl.patternType() == PREFIXED;

    /**
     * Creates a predicate that will match the resource pattern type requried by the action against the node.
     * @param node The node to check.
     * @param action the action to find.
     * @return either {@code literalPattern} or {@code prefixPattern} depending on whether or not the node fragment is a wildcard.
     */
    Predicate<StandardAcl> resourcePatternFilter(Node<NameTriePattern> node, Action action) {
        return WildcardRegistry.isWildcard(node.getFragment()) || action.resourcePattern().name().endsWith(node.getFragment()) ? literalPattern : prefixedPattern;
    }

    /**
     * Creates a Predicate that matches the host or the wildcard.
     * @param host the host to match.
     * @return the Predicate that matches the host pattern or wildcard.
     */
    Predicate<StandardAcl> hostFilter(final String host) {
        return  acl -> acl.host().equals(WILDCARD) || acl.host().equals(host);
    }

    /**
     * Create a predicate that matches the operation depending on the ACL operation and the action operation.
     * This method performs the expansion of the ALL in the ACL to process DESCRIBE and DESCRIBE_CONFIGS correctly.
     * @param op The Acl operation
     * @return the Predicate that matches the ACL operation.
     */
    Predicate<StandardAcl> operationFilter(final AclOperation op) {
        // operation filter
        return acl -> {
            if (acl.operation() != ALL) {
                if (acl.permissionType().equals(ALLOW)) {
                    switch (op) {
                        case DESCRIBE:
                            return BitMaps.contains(DESCRIBE_MAP, acl.operation().code());
                        case DESCRIBE_CONFIGS:
                            return BitMaps.contains(CONFIGS_MAP, acl.operation().code());
                        default:
                            return op == acl.operation();
                    }
                } else {
                    return op == acl.operation();
                }
            }
            return true;
        };
    }

    public AuthorizationResult authorizeByResourceType(KafkaPrincipal principal, String host, AclOperation op, ResourceType resourceType) {
        MatchingRule rule = matchingRuleByResourceType(principal, host, op, resourceType);
        logAuditMessage(principal, host, op, resourceType, rule);
        return rule.result();
    }
    private MatchingRule matchingRuleByResourceType(KafkaPrincipal principal, String host, AclOperation op, ResourceType resourceType) {
        NameTrie<NameTriePattern> trie = trieData.getTrie(resourceType);
        // principal filter
        final Predicate<StandardAcl> principalFilter = acl -> principal.equals(acl.kafkaPrincipal());
        final Predicate<StandardAcl> aclFilter = operationFilter(op).and(hostFilter(host)).and(principalFilter);
        final Predicate<StandardAcl> filter = aclFilter.and(principalFilter);


        @SuppressWarnings("unchecked")
        final Node<NameTriePattern> found[] = new Node[1];
        Predicate<Node<NameTriePattern>> traversalFilter = n -> {
            NameTriePattern contents = n.getContents();
            if (contents != null) {
                Optional<StandardAcl> opt = contents.first(filter);
                if (opt.isPresent()) {
                    found[0] = n;
                    return false;
                }
            }
            return true;
        };

        Walker.inOrder(traversalFilter, trieData.getTrie(resourceType).getRoot());

        if (found[0] != null) {
            Optional<StandardAcl> optionalAcl = found[0].getContents().first(filter);
            if (optionalAcl.isPresent()) {
                log.info("ACL found {}", optionalAcl);
                return matchingRuleFromOptionalAcl(optionalAcl);
            }
        }
        log.info("No ACL found -- returning DENY");
        return MatchingRuleBuilder.DENY_RULE;
    }


    private MatchingRule findAclRule(
            Set<KafkaPrincipal> matchingPrincipals,
            String host,
            Action action
    ) {

        // principal filter
        final Predicate<StandardAcl> principalFilter = acl -> matchingPrincipals.contains(acl.kafkaPrincipal());

        // acl filter = operation + host + principal
        final Predicate<StandardAcl> aclFilter = operationFilter(action.operation()).and(hostFilter(host)).and(principalFilter);

        // stop searching when we hit a valid DENY.
        Predicate<StandardAcl> permissionFilter = acl -> acl.permissionType() == DENY;

        final Predicate<StandardAcl> exitFilter = permissionFilter.and(aclFilter);
        Node<NameTriePattern> target = trieData.findNode(action.resourcePattern(), n -> check(n, exitFilter, action).isPresent());
        // if the root of the tree -> no matches so return noAclRule
        if (target.getFragment().equals("")) {
            log.info("No match for {} {} {}", action, host, matchingPrincipals);
            return noAclRule;
        }
        log.info("Search returned {} {} {}", target, action, host);

        // see if we have a match or hit a matching DENY then this will find it.
        Optional<StandardAcl> optionalAcl = check(target, aclFilter, action);
        if (optionalAcl.isPresent()) {
            log.info("Matching ACL found {}", optionalAcl);
            return matchingRuleFromOptionalAcl(optionalAcl);
        }

        // if the target does not have matching rule then move back up the path looking for a matching node.
        permissionFilter = acl -> acl.permissionType() == ALLOW;
        final Predicate<StandardAcl> acceptFilter = permissionFilter.and(prefixedPattern).and(aclFilter);
        while (!target.getFragment().equals("")) {
            NameTriePattern pattern = target.getContents();
            if (pattern != null) {
                optionalAcl = pattern.first(ALLOW, acceptFilter);
                if (optionalAcl.isPresent()) {
                    log.info("Prefixed ACL found {}", optionalAcl);
                    return matchingRuleFromOptionalAcl(optionalAcl);
                }
            }
            // scan up the tree looking for Prefix matches.
            target = target.getParent();
        }
        log.info("No ACL found -- returning DENY");
        return MatchingRuleBuilder.DENY_RULE;
    }

    private MatchingRule matchingRuleFromOptionalAcl(Optional<StandardAcl> optionalAcl) {
        StandardAcl acl = optionalAcl.get();
        return new MatchingAclRule(acl, acl.permissionType().equals(ALLOW) ? ALLOWED : DENIED);
    }

    private void logAuditMessage(
            KafkaPrincipal principal,
            AuthorizableRequestContext requestContext,
            Action action,
            MatchingRule rule
    ) {
        switch (rule.result()) {
            case ALLOWED:
                // logIfAllowed is true if access is granted to the resource as a result of this authorization.
                // In this case, log at debug level. If false, no access is actually granted, the result is used
                // only to determine authorized operations. So log only at trace level.
                if (action.logIfAllowed() && auditLog.isDebugEnabled()) {
                    auditLog.debug(buildAuditMessage(principal, requestContext, action, rule));
                } else if (auditLog.isTraceEnabled()) {
                    auditLog.trace(buildAuditMessage(principal, requestContext, action, rule));
                }
                return;

            case DENIED:
                // logIfDenied is true if access to the resource was explicitly requested. Since this is an attempt
                // to access unauthorized resources, log at info level. If false, this is either a request to determine
                // authorized operations or a filter (e.g for regex subscriptions) to filter out authorized resources.
                // In this case, log only at trace level.
                if (action.logIfDenied()) {
                    auditLog.info(buildAuditMessage(principal, requestContext, action, rule));
                } else if (auditLog.isTraceEnabled()) {
                    auditLog.trace(buildAuditMessage(principal, requestContext, action, rule));
                }
        }
    }
    private void logAuditMessage(
            //logAuditMessage(principal, host, op, resourceType, rule);
            KafkaPrincipal principal,
            String host,
            AclOperation op,
            ResourceType resourceType,
            MatchingRule rule
    ) {
        switch (rule.result()) {
            case ALLOWED:
                if (auditLog.isTraceEnabled()) {
                    auditLog.trace(buildAuditMessage(principal, host, op, resourceType, rule));
                }
                return;

            case DENIED:
                if (auditLog.isTraceEnabled()) {
                    auditLog.trace(buildAuditMessage(principal, host, op, resourceType, rule));
                }
        }
    }

    private String buildAuditMessage(
            KafkaPrincipal principal,
            String host,
            AclOperation op,
            ResourceType resourceType,
            MatchingRule rule
    ) {
        StringBuilder bldr = new StringBuilder();
        bldr.append("Principal = ").append(principal);
        bldr.append(" is ").append(rule.result() == ALLOWED ? "Allowed" : "Denied");
        bldr.append(" operation = ").append(op);
        bldr.append(" from host = ").append(host);
        bldr.append(" for resource type = ").append(resourceType.name());
        bldr.append(" based on rule ").append(rule);
        return bldr.toString();
    }

    private String buildAuditMessage(
            KafkaPrincipal principal,
            AuthorizableRequestContext context,
            Action action,
            MatchingRule rule
    ) {
        StringBuilder bldr = new StringBuilder();
        bldr.append("Principal = ").append(principal);
        bldr.append(" is ").append(rule.result() == ALLOWED ? "Allowed" : "Denied");
        bldr.append(" operation = ").append(action.operation());
        bldr.append(" from host = ").append(context.clientAddress().getHostAddress());
        bldr.append(" on resource = ");
        appendResourcePattern(action.resourcePattern(), bldr);
        bldr.append(" for request = ").append(ApiKeys.forId(context.requestType()).name);
        bldr.append(" with resourceRefCount = ").append(action.resourceReferenceCount());
        bldr.append(" based on rule ").append(rule);
        return bldr.toString();
    }

    private void appendResourcePattern(ResourcePattern resourcePattern, StringBuilder bldr) {
        bldr.append(SecurityUtils.resourceTypeName(resourcePattern.resourceType()))
                .append(":")
                .append(resourcePattern.patternType())
                .append(":")
                .append(resourcePattern.name());
    }

    private static class AclContainer {

        Comparator<AclPermissionType> permissionOrder = new Comparator<AclPermissionType>() {
            @Override
            public int compare(AclPermissionType aclPermissionType, AclPermissionType other) {
                if (aclPermissionType == DENY) {
                    return other == DENY ? 0 : -1;
                }
                return other == DENY ? 1 : aclPermissionType.compareTo(other);
            }
        };

        Comparator<StandardAcl> partialOrder = (a, b) -> {
            int result = permissionOrder.compare(a.permissionType(), b.permissionType());
            if (result != 0) return result;
            result = a.patternType().compareTo(b.patternType());
            if (result != 0) return result;
            result = a.operation().compareTo(b.operation());
            if (result != 0) return result;
            result = a.principal().compareTo(b.principal());
            if (result != 0) return result;
            result = a.host().compareTo(b.host());
            return result;
        };

        private final SortedSet<StandardAcl> partialAcls;

        AclContainer(StandardAcl acl) {
            partialAcls = new TreeSet<>(partialOrder);
            partialAcls.add(acl);
        }

        AclContainer(Collection<StandardAcl> acls) {
            partialAcls = new TreeSet<>(partialOrder);
            partialAcls.addAll(acls);
        }


        public void add(StandardAcl acl) {
            partialAcls.add(acl);
        }

        public void remove(StandardAcl acl) {
            partialAcls.remove(acl);
        }


        public boolean isEmpty() {
            return partialAcls.isEmpty();
        }


        public Optional<StandardAcl> first(Predicate<StandardAcl> filter) {
            for (StandardAcl acl : partialAcls) {
                if (filter.test(acl)) {
                    return Optional.of(acl);
                }
            }
            return Optional.empty();
        }

        public Optional<StandardAcl> first(AclPermissionType permission, Predicate<StandardAcl> filter) {
            StandardAcl exemplar = new StandardAcl(
                    null,
                    null,
                    PatternType.UNKNOWN, // Note that the UNKNOWN value sorts before all others.
                    "",
                    "",
                    AclOperation.UNKNOWN,
                    permission);
            for (StandardAcl candidate : partialAcls.tailSet(exemplar)) {
                if (filter.test(candidate)) {
                    return Optional.of(candidate);
                }
            }
            return Optional.empty();
        }
    }


    private static class NameTriePattern {

        AclContainer acls;

        NameTriePattern(StandardAcl acl) {
            acls = new AclContainer(acl);

        }

        public void add(StandardAcl acl) {
            acls.add(acl);
        }

        public void remove(StandardAcl acl) {
            acls.remove(acl);
        }

        public boolean isEmpty() {
            return acls.isEmpty();
        }

        public Optional<StandardAcl> first(Predicate<StandardAcl> filter) {
            return acls.first(filter);
        }

        public Optional<StandardAcl> first(AclPermissionType permission, Predicate<StandardAcl> filter) {
            return acls.first(permission, filter);
        }
    }

    private static class TrieData {
        private final Map<ResourceType, NameTrie<NameTriePattern>> tries;

        private final Map<Uuid, StandardAcl> uuidMap;


        private static final Logger log = LoggerFactory.getLogger(NameTriePattern.class);

        TrieData() {
            log.info("Constructing TrieData");
            tries = new HashMap<>();
            uuidMap = new HashMap<>();
        }

        TrieData(Map<Uuid, StandardAcl> acls) {
            this();
            for (Map.Entry<Uuid, StandardAcl> entry : acls.entrySet()) {
                add(entry.getKey(), entry.getValue());
            }
        }

        public void add(Uuid uuid, StandardAcl acl) {
            uuidMap.put(uuid, acl);
            NameTrie<NameTriePattern> trie = tries.get(acl.resourceType());
            if (trie == null) {
                log.info("creating trie for resource type {}.", acl.resourceType());
                trie = new NameTrie<>();
                tries.put(acl.resourceType(), trie);
                NameTriePattern pattern = new NameTriePattern(acl);
                trie.put(acl.resourceName(), pattern);
            } else {
                NameTriePattern pattern = trie.get(acl.resourceName());
                if (pattern == null) {
                    pattern = new NameTriePattern(acl);
                    trie.put(acl.resourceName(), pattern);
                } else {
                    pattern.add(acl);
                }
            }
        }

        public void remove(Uuid uuid) {
            StandardAcl acl = uuidMap.get(uuid);
            if (acl != null) {
                uuidMap.remove(uuid);
                NameTrie<NameTriePattern> trie = tries.get(acl.resourceType());
                if (trie != null) {
                    log.debug("removing trie entry for " + acl);
                    NameTriePattern pattern = trie.get(acl.resourceName());
                    if (pattern != null) {
                        pattern.remove(acl);
                        if (pattern.isEmpty()) {
                            trie.remove(acl.resourceName());
                        }
                    }

                }
            }
        }

        public Iterable<AclBinding> acls(AclBindingFilter filter) {
            //return uuidMap.values().stream().map(StandardAcl::toBinding).filter(filter::matches).collect(Collectors.toList());
            List<AclBinding> aclBindingList = new ArrayList<>();
            uuidMap.values().forEach(acl -> {
                AclBinding aclBinding = acl.toBinding();
                if (filter.matches(aclBinding)) {
                    aclBindingList.add(aclBinding);
                }
            });
            return aclBindingList;
        }

        public int count() {
            return uuidMap.size();
        }

        /**
         * Finds the node in the proper tree.
         * @param resourcePattern the resource pattern Trie to search.
         * @param exit the predicate that forces a stop/exit from the search.
         * @return the Node that matches or caused an exit.
         */
        public Node<NameTriePattern> findNode(ResourcePattern resourcePattern, Predicate<Node<NameTriePattern>> exit) {
            NameTrie<NameTriePattern> trie = tries.get(resourcePattern.resourceType());
            if (trie == null) {
                log.info("No trie found for {}", resourcePattern.resourceType());
                return Node.makeRoot();
            }
            Node<NameTriePattern> n = trie.findNode(resourcePattern.name(), exit);
            log.debug("Returning {}.", n);
            return n;
        }

        public NameTrie<NameTriePattern> getTrie(ResourceType resourceType) {
            return tries.get(resourceType);
        }
    }

}
