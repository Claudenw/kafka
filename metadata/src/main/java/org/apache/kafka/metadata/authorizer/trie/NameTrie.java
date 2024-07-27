package org.apache.kafka.metadata.authorizer.trie;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.IntConsumer;
import java.util.function.Predicate;

/**
 * The NameTrie is a Trie structure indexed by strings.
 * This trie is simplar to a Patricia trie in that some internal nodes have data associated with them.
 * @param <T> the type of object held in the trie.
 */
public class NameTrie<T>  {
    /** the root node */
    private Node<T> root;
    /** the size of the trie in populated nodes */
    private int size = 0;

    /** A predicate to just execute a find without processing intermediate steps */
    private final Predicate<Node<T>> noExit = (x) -> false;

    private final IntConsumer counter = i -> size += i;

    /**
     * Constructor.
     * Creates an empty tree.
     */
    public NameTrie() {
        clear();
    }

    /**
     * Clear the contents of the trie.
     */
    public void clear() {
        root = Node.makeRoot();
        size = 0;
    }

    /**
     * Find the list of nodes from the root to the node that matches the value or the last node the trie that would
     * be on the path to the matching node if it existed.
     * @param value the name to look for.
     * @return the list of Nodes on the path.  Will be an empty list if value was not found.
     */
    public List<Node<T>> pathTo(String value) {
        Node<T> n = findNode(value);
        return n == null ? Collections.emptyList() : n.pathTo();
    }

    /**
     * The number of populated nodes in the tree.
     * @return the number of nodes with contents in the tree.
     */
    public int size() {
        return size;
    }

    /**
     * Returns true if {@code size() == 0}
     * @return true if there are no populated nodes in the tree.
     */
    public boolean isEmpty() {
        return size == 0;
    }

    /**
     * Gets the root.
     * @return Returns root node of the trie
     */
    public Node<T> getRoot() {
        return root;
    }


    /**
     * Finds a node.  A longer value will match a shorter segment.  For example "HotWheels" will match "HotWheel"
     * but not vice versa.
     * @param value the name to look for.
     * @return Returns the matching node or null if no matching node is found.
     */
    public Node<T> findNode(String value) {
        return findNode(value, noExit);
    }

    /**
     * Finds a node.  A longer value will match a shorter segment.  For example "HotWheels" will match "HotWheel"
     * but not vice versa.
     *
     * If the {@code exit} predicate returns true on any node during the traversal of the tree, the traversal is stopped and
     * the current node is returned.
     *
     * Will return the root node if no match is found.
     *
     * @param value the name to look for.
     * @param exit A predicate that is tested for each node on the traversal from the root.
     * @return Returns the matching node or the last node the trie that would be on the path to the matching node if it existed,
     * or the node on which {@code exit} returned {@code true}.
     */
    public Node<T> findNode(String value, Predicate<Node<T>> exit) {
        return root.findNodeFor(new StringMatcher<T>(value, exit));
    }

    /**
     * Gets the object from the trie.
     * @param key the key to search for.
     * @return The stored object or {@code null} if not found.
     */
    public T get(String key) {
        Node<T> n = findNode(key, noExit);
        return (n != null && key.endsWith(n.getFragment())) ? n.getContents() : null;
    }

    /**
     * Adds a node or returns an existing node.
     *
     * @param key the key for the node.
     * @return Returns the matching or new node.
     */
    public Node<T> addNode(String key) {
        return root.addNodeFor(counter, new StringInserter(key));
    }

    /**
     * Puts an object in the trie with the specified key.
     * @param key the key.
     * @param object the object.
     * @return the previous value for the key or {@code null} if no previous value existed.
     */
    public T put(String key, T object) {
        return addNode(key).setContents(object);
    }

    /**
     * Put a map of objects into the trie.
     * @param map the maps to add.
     */
    public void putAll(Map<? extends String, ? extends T> map) {
        for (Map.Entry<? extends String, ? extends T> entry : map.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    /**
     * Remove an object from the trie.
     * @param key the key that identifies the object to remove.
     * @return the contents of the node if it had any.
     */
    public T remove(String key) {
        Node<T> n = findNode(key);
        if (n != null && key.endsWith(n.getFragment())) {
            T result = n.getContents();
            n.delete(counter);
            return result;
        }
        return null;
    }
}
