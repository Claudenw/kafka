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

public class StringMatcher<T> implements Matcher<T> {
    private final StringMatcherBase<T> base;

    private final int position;


    public StringMatcher(String pattern, Predicate<Node<T>> exit) {
        this(new StringMatcherBase<>(pattern, exit), 0);
    }

    private StringMatcher(StringMatcherBase<T> base, int position)
    {
        this.base = base;
        this.position = position;
    }

    public String getFragment() {
        return this.base.pattern.substring(position);
    }

    public StringMatcher<T> advance(int advance) {
        return new StringMatcher<>(base, position+advance);
    }

    @Override
    public boolean test(Node<T> node) {
        return base.exit.test(node);
    }

    private static class StringMatcherBase<T> {
        private final String pattern;
        private final Predicate<Node<T>> exit;

        private StringMatcherBase(String pattern, Predicate<Node<T>> exit) {
            this.pattern = pattern;
            this.exit = exit;
        }
    }
}
