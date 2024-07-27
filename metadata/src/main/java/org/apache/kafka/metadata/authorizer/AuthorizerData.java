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
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.slf4j.Logger;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.common.acl.AclOperation.ALL;
import static org.apache.kafka.common.acl.AclOperation.ALTER;
import static org.apache.kafka.common.acl.AclOperation.ALTER_CONFIGS;
import static org.apache.kafka.common.acl.AclOperation.DELETE;
import static org.apache.kafka.common.acl.AclOperation.DESCRIBE;
import static org.apache.kafka.common.acl.AclOperation.DESCRIBE_CONFIGS;
import static org.apache.kafka.common.acl.AclOperation.READ;
import static org.apache.kafka.common.acl.AclOperation.WRITE;
import static org.apache.kafka.common.acl.AclPermissionType.ALLOW;
import static org.apache.kafka.server.authorizer.AuthorizationResult.ALLOWED;
import static org.apache.kafka.server.authorizer.AuthorizationResult.DENIED;

public interface AuthorizerData {
    /**
     * The host or name string used in ACLs that match any host or name.
     */
    String WILDCARD = "*";
    /**
     * The principal entry used in ACLs that match any principal.
     */
    String WILDCARD_PRINCIPAL = "User:*";
    KafkaPrincipal WILDCARD_KAFKA_PRINCIPAL = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "*");
    /**
     * The set of operations which imply DESCRIBE permission, when used in an ALLOW acl.
     */
    Set<AclOperation> IMPLIES_DESCRIBE = Collections.unmodifiableSet(
        EnumSet.of(DESCRIBE, READ, WRITE, DELETE, ALTER));
    /**
     * The set of operations which imply DESCRIBE_CONFIGS permission, when used in an ALLOW acl.
     */
    Set<AclOperation> IMPLIES_DESCRIBE_CONFIGS = Collections.unmodifiableSet(
        EnumSet.of(DESCRIBE_CONFIGS, ALTER_CONFIGS));

    static AuthorizationResult findResult(Action action,
                                          AuthorizableRequestContext requestContext,
                                          StandardAcl acl) {
        return findResult(
            action,
            matchingPrincipals(requestContext),
            requestContext.clientAddress().getHostAddress(),
            acl
        );
    }

    static KafkaPrincipal baseKafkaPrincipal(AuthorizableRequestContext context) {
        KafkaPrincipal sessionPrincipal = context.principal();
        return sessionPrincipal.getClass().equals(KafkaPrincipal.class)
            ? sessionPrincipal
            : new KafkaPrincipal(sessionPrincipal.getPrincipalType(), sessionPrincipal.getName());
    }

    static Set<KafkaPrincipal> matchingPrincipals(AuthorizableRequestContext context) {
        KafkaPrincipal sessionPrincipal = context.principal();
        KafkaPrincipal basePrincipal = sessionPrincipal.getClass().equals(KafkaPrincipal.class)
            ? sessionPrincipal
            : new KafkaPrincipal(sessionPrincipal.getPrincipalType(), sessionPrincipal.getName());
        return Utils.mkSet(basePrincipal, WILDCARD_KAFKA_PRINCIPAL);
    }

    /**
     * Determine what the result of applying an ACL to the given action and request
     * context should be. Note that this function assumes that the resource name matches;
     * the resource name is not checked here.
     *
     * @param action             The input action.
     * @param matchingPrincipals The set of input matching principals
     * @param host               The input host.
     * @param acl                The input ACL.
     * @return                   null if the ACL does not match. The authorization result
     *                           otherwise.
     */
    static AuthorizationResult findResult(Action action,
                                          Set<KafkaPrincipal> matchingPrincipals,
                                          String host,
                                          StandardAcl acl) {
        // Check if the principal matches. If it doesn't, return no result (null).
        if (!matchingPrincipals.contains(acl.kafkaPrincipal())) {
            return null;
        }
        // Check if the host matches. If it doesn't, return no result (null).
        if (!acl.host().equals(WILDCARD) && !acl.host().equals(host)) {
            return null;
        }
        // Check if the operation field matches. Here we hit a slight complication.
        // ACLs for various operations (READ, WRITE, DELETE, ALTER), "imply" the presence
        // of DESCRIBE, even if it isn't explicitly stated. A similar rule applies to
        // DESCRIBE_CONFIGS.
        //
        // But this rule only applies to ALLOW ACLs. So for example, a DENY ACL for READ
        // on a resource does not DENY describe for that resource.
        if (acl.operation() != ALL) {
            if (acl.permissionType().equals(ALLOW)) {
                switch (action.operation()) {
                    case DESCRIBE:
                        if (!IMPLIES_DESCRIBE.contains(acl.operation())) return null;
                        break;
                    case DESCRIBE_CONFIGS:
                        if (!IMPLIES_DESCRIBE_CONFIGS.contains(acl.operation())) return null;
                        break;
                    default:
                        if (action.operation() != acl.operation()) {
                            return null;
                        }
                        break;
                }
            } else if (action.operation() != acl.operation()) {
                return null;
            }
        }

        return acl.permissionType().equals(ALLOW) ? ALLOWED : DENIED;
    }


    AuthorizerData copyWithNewLoadingComplete(boolean newLoadingComplete);

    AuthorizerData copyWithNewConfig(int nodeId,
                                             Set<String> newSuperUsers,
                                             AuthorizationResult newDefaultResult) ;

    AuthorizerData copyWithNewAcls(Map<Uuid, StandardAcl> acls);

    AuthorizerData copyWithNewAclMutator(AclMutator newAclMutator);

    void addAcl(Uuid id, StandardAcl acl);

    void removeAcl(Uuid id);

    Set<String> superUsers();

    AuthorizationResult defaultResult();

    int aclCount();

    AclMutator aclMutator();

    Logger log();

    Iterable<AclBinding> acls(AclBindingFilter filter);

    AuthorizationResult authorize(
            AuthorizableRequestContext requestContext,
            Action action
    );

    interface MatchingRule {
        AuthorizationResult result();
    }

    class SuperUserRule implements MatchingRule {
        static final SuperUserRule INSTANCE = new SuperUserRule();

        @Override
        public AuthorizationResult result() {
            return ALLOWED;
        }

        @Override
        public String toString() {
            return "SuperUser";
        }
    }

    class DefaultRule implements MatchingRule {
        private final AuthorizationResult result;

        DefaultRule(AuthorizationResult result) {
            this.result = result;
        }

        @Override
        public AuthorizationResult result() {
            return result;
        }

        @Override
        public String toString() {
            return result == ALLOWED ? "DefaultAllow" : "DefaultDeny";
        }
    }

    class MatchingAclRule implements MatchingRule {
        private final StandardAcl acl;
        private final AuthorizationResult result;

        MatchingAclRule(StandardAcl acl, AuthorizationResult result) {
            this.acl = acl;
            this.result = result;
        }

        @Override
        public AuthorizationResult result() {
            return result;
        }

        @Override
        public String toString() {
            return "MatchingAcl(acl=" + acl + ")";
        }
    }

    class MatchingRuleBuilder {
        static final DefaultRule DENY_RULE = new DefaultRule(DENIED);
        private final DefaultRule noAclRule;
        StandardAcl denyAcl;
        StandardAcl allowAcl;
        boolean hasResourceAcls;

        public MatchingRuleBuilder(DefaultRule noAclRule) {
            this.noAclRule = noAclRule;
        }

        boolean foundDeny() {
            return denyAcl != null;
        }

        MatchingRule build() {
            if (denyAcl != null) {
                return new MatchingAclRule(denyAcl, DENIED);
            } else if (allowAcl != null) {
                return new MatchingAclRule(allowAcl, ALLOWED);
            } else if (!hasResourceAcls) {
                return noAclRule;
            } else {
                return DENY_RULE;
            }
        }
    }
}
