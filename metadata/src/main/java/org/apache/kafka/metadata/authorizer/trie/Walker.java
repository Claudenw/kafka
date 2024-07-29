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
package org.apache.kafka.metadata.authorizer.trie;

import java.util.function.Predicate;

/**
 * An object that walks the Trie in specific order applying a predicate.
 * <p>
 *     The predicates are intended to perform some action on the node and return true if the walk should continue, false
 *     otherwise.
 *
 *     As an example a predicate that counted nodes in the trie could be implemented as follows:
 *
 *     <pre>
 *          final int[] count = new int[] { 0 };
 *          Predicate<Node<T>> pred = n -> {
 *              count[0]++;
 *              return true;
 *         }
 *     </pre>
 * </p>
 *
 * @see <a href='https://en.wikipedia.org/wiki/Tree_traversal'>Tree Traversal [Wikipedia]</a>
 */
public class Walker {

    /**
     * Applies the predicate to each node in a depth-first fashion.
     * If the predicate returns false the walker will stop.
     * @param predicate the Predicate to apply.
     * @param data the Node to start at.
     * @return {@code true} if the predicate was applied to all the nodes, {@code false} otherwise.
     *
     */
    public static <T> boolean depthFirst(Predicate<Node<T>> predicate, Node<T> data) {
        if (data.getChildren() != null) {
            for (Node<T> child : data.getChildren()) {
                if (!depthFirst(predicate, child)) {
                    return false;
                }
            }
        }
        return predicate.test(data);
    }

    /**
     * Applies the predicate to each node in a pre-order fashion.
     * If the predicate returns false the walker will stop.
     * @param predicate the Predicate to apply.
     * @param data the Node to start at.
     * @return {@code true} if the predicate was applied to all the nodes, {@code false} otherwise.
     */
    public static <T> boolean preOrder(Predicate<Node<T>> predicate, Node<T> data) {
        if (!predicate.test(data)) {
            return false;
        }
        if (data.getChildren() != null) {
            for (Node<T> child : data.getChildren()) {
                if (!preOrder(predicate, child)) {
                    return false;
                }
            }
        }
        return true;
    }
}
