package com.type.ahead.search.util;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

@Slf4j
@Setter
@Getter
public class TrieDataStore {
    TrieNode root;
    private static TrieDataStore trie_DataStore_instance = null;

    private Set<String> allWords = new HashSet<>();

    private TrieDataStore() {

    }

    public static TrieDataStore getTrieInstance() {
        if (trie_DataStore_instance == null) {
            trie_DataStore_instance = new TrieDataStore();
        }
        return trie_DataStore_instance;
    }

    public void reset() {
        root = new TrieNode();
        allWords = new HashSet<>();
        TrieNode.totalNodes = 0;
    }

    public void TrieLoadData(List<String> words) {
        root = new TrieNode();
        for (String word : words) {
            allWords.add(word);
            root.insert(word);
        }
    }

    public void insertWord(String word) {
        allWords.add(word);
        if (root == null) {
            root = new TrieNode();
        }
        root.insert(word);
    }

    public void suggestHelper(TrieNode root, List<String> list, StringBuffer curr) {
        if (root.isWord) {
            list.add(curr.toString());
        }

        if (root.children == null || root.children.isEmpty())
            return;

        for (TrieNode child : root.children.values()) {
            suggestHelper(child, list, curr.append(child.c));
            curr.setLength(curr.length() - 1);
        }
    }

    public List<String> suggest(String prefix) {
        List<String> list = new ArrayList<>();
        TrieNode lastNode = root;
        StringBuffer curr = new StringBuffer();
        for (char c : prefix.toCharArray()) {
            lastNode = lastNode.children.get(c);
            if (lastNode == null)
                return list;
            curr.append(c);
        }
        suggestHelper(lastNode, list, curr);
        return list;
    }

    public Set<String> allPrefixes() {
        Set<String> prefixes = new HashSet<>();
        for (String word : allWords)
            prefixes.addAll(extractAllPrefixes(word));
        return prefixes;

    }

    private Collection<String> extractAllPrefixes(String word) {
        List<String> prefixes = new ArrayList<>();
        for (int i = 1; i <= word.length(); i++) {
            prefixes.add(word.substring(0, i));
        }
        return prefixes;
    }

    public static int getTrieSize() {
        return TrieNode.totalNodes;
    }
}
