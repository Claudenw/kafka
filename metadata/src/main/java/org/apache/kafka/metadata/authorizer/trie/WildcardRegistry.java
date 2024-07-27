package org.apache.kafka.metadata.authorizer.trie;

import java.util.SortedSet;

import static java.lang.String.format;

public class WildcardRegistry {
    /* list of wildcard strings.  Order is important.  Most specific match first */
    public static final String[] PATTERNS = {"?", "*"};

    public static boolean isWildcard(String s) {
        return s.length() == 1 && isWildcard(s.charAt(0));
    }

    public static boolean isWildcard(char c) {
        return "*?".indexOf(c) > -1;
    }

    public static String getSegment(String pattern) {
        int splat = pattern.indexOf('*');
        int quest = pattern.indexOf('?');

        splat = splat == -1 ? quest : splat;
        quest = quest == -1 ? splat : quest;
        int pos = Math.min(splat, quest);
        if (pos == 0) {
            return pattern.substring(0, 1);
        }
        return pos == -1 ? pattern : pattern.substring(0, pos);
    }

    public static <T> Node<T> processWildcards(Node<T> parent, Matcher<T> matcher) {
        Node<T>.Search searcher = parent.new Search();
        Node<T> match = null;

        for (String wildcard : PATTERNS) {
            Node<T> child = searcher.eq(() -> wildcard);
            if (child != null) {
                switch (wildcard) {
                    case "?":
                        match = child.findNodeFor(matcher.advance(1));
                        if (Node.validMatch(match)) {
                            return match;
                        }
                        break;
                    case "*":
                        for (int advance = 1; advance < matcher.getFragment().length(); advance++) {
                            match = child.findNodeFor(matcher.advance(advance));
                            if (Node.validMatch(match)) {
                                return match;
                            }
                        }
                        break;
                    default:
                        throw new IllegalArgumentException(format("'%s' is not a valid wildcard", wildcard));
                }
            }
        }
        return null;
    }
}
