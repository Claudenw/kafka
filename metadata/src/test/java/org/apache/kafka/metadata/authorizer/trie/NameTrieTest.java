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

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class NameTrieTest {

    private NameTrie<Integer> standardSetup() {
        NameTrie<Integer> trie = new NameTrie<>();
        trie.put("onetwothree", 3);
        trie.put("onethreefour", 4);
        trie.put("twothreefive", 5);
        return trie;
    }

    @Test
    public void testEquality() {
        NameTrie<Integer> trie = new NameTrie<>();
        assertTrue(trie.isEmpty());
        trie.put("onetwothree", 3);
        assertFalse(trie.isEmpty());
        assertEquals(1, trie.size());
        trie.put("onethreefour", 4);
        assertEquals(2, trie.size());
        trie.put("twothreefive", 5);
        assertEquals(3, trie.size());

        assertEquals(Integer.valueOf(3), trie.get("onetwothree"));

        Node<Integer> n = trie.findNode("onetwothree");
        assertEquals("onetwothree", n.toString());
        List<Node<Integer>> lst = n.pathTo();
        assertEquals(2, lst.size());
        assertEquals("onet", lst.get(0).getFragment());
        assertEquals("wothree", lst.get(1).getFragment());
        assertEquals(n, lst.get(1));

        assertEquals(lst, trie.pathTo("onetwothree"));
    }

    @Test
    public void testPartialSearch() {
        NameTrie<Integer> trie = new NameTrie<>();
        trie.put("HotWheels", 1);
        trie.put("HatWheels", 2);
        trie.put("HotMama", 3);


        Node<Integer> n = trie.findNode("HotWheelsCar");
        assertEquals("HotWheels", n.toString());

        n = trie.findNode("HotWhee");
        assertEquals("", n.toString());
    }

    @Test
    public void getTest() {
        NameTrie<Integer> trie = standardSetup();

        assertEquals(3, trie.get("onetwothree"));
        assertNull(trie.get("onetwoth"));
        assertNull(trie.get("onet"));
    }
}
