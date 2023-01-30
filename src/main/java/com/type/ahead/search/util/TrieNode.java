package com.type.ahead.search.util;

import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Slf4j
public class TrieNode {
    public static int totalNodes;
    Map<Character, TrieNode> children;
    char c;
    boolean isWord;
    int score;

    public TrieNode(char c) {
        this.c = c;
        totalNodes++;                                           // Increment totalNodes when new node is created
        children = new HashMap<>();
    }

    public TrieNode() {
        this.children = new HashMap<>();
    }

    public void insert(String word) {
        if (word == null || word.isEmpty()) return;
        char firstChar = word.charAt(0);
        TrieNode child = children.get(firstChar);
        if (child == null) {
            // create a new node
            child = new TrieNode(firstChar);
            children.put(firstChar, child);
        }
        if (word.length() > 1) child.insert(word.substring(1));
        else child.isWord = true;
    }

}



