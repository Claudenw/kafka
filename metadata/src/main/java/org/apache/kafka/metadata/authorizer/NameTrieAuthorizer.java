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

import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.SecurityUtils;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;

import java.util.Collections;

/**
 * An Authorizer that extends the standard Authorizer by reimplementing the
 * {@link #authorizeByResourceType(AuthorizableRequestContext, AclOperation, ResourceType)}
 * method and providing a Trie implementation for the {@link AuthorizerData}.
 */
public class NameTrieAuthorizer extends StandardAuthorizer {

    /**
     * Constructor.
     */
    public NameTrieAuthorizer() {
        super(NameTrieAuthorizerData.createEmpty());
    }

    /**
     * Check if the caller is authorized to perform the given ACL operation on at least one
     * resource of the given type.
     *
     * Custom authorizer implementations should consider overriding this default implementation because:
     * 1. The default implementation iterates all AclBindings multiple times, without any caching
     *    by principal, host, operation, permission types, and resource types. More efficient
     *    implementations may be added in custom authorizers that directly access cached entries.
     * 2. The default implementation cannot integrate with any audit logging included in the
     *    authorizer implementation.
     * 3. The default implementation does not support any custom authorizer configs or other access
     *    rules apart from ACLs.
     *
     * @param requestContext Request context including request resourceType, security protocol and listener name
     * @param op             The ACL operation to check
     * @param resourceType   The resource type to check
     * @return               Return {@link AuthorizationResult#ALLOWED} if the caller is authorized
     *                       to perform the given ACL operation on at least one resource of the
     *                       given type. Return {@link AuthorizationResult#DENIED} otherwise.
     */
    public AuthorizationResult authorizeByResourceType(AuthorizableRequestContext requestContext, AclOperation op, ResourceType resourceType) {

        SecurityUtils.authorizeByResourceTypeCheckArgs(op, resourceType);

        // Check a hard-coded name to ensure that super users are granted
        // access regardless of DENY ACLs.
        if (authorize(requestContext, Collections.singletonList(new Action(
                op, new ResourcePattern(resourceType, "hardcode", PatternType.LITERAL),
                0, true, false)))
                .get(0) == AuthorizationResult.ALLOWED) {
            return AuthorizationResult.ALLOWED;
        }

        KafkaPrincipal principal = new KafkaPrincipal(
                requestContext.principal().getPrincipalType(),
                requestContext.principal().getName());
        String host = requestContext.clientAddress().getHostAddress();

        NameTrieAuthorizerData curData = (NameTrieAuthorizerData) getData();
        return  curData.authorizeByResourceType(principal, host, op, resourceType);

    }
}
