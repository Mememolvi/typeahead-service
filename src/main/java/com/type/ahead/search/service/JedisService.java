package com.type.ahead.search.service;


import com.type.ahead.search.util.TrieDataStore;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;

import javax.annotation.PostConstruct;
import java.util.*;

@Service
@Slf4j
public class JedisService {
    @Value("${common.word.list}")
    List<String> wordList;

    @Value("${redis.sorted.set.size}")
    Integer redisSetSize;

    @Value("${redis.retain.ratio}")
    Integer redisRetainRatio;

    @Value("${data.load.size}")
    Integer dataLoadSize;
    private final TrieDataStore trieDataStore = TrieDataStore.getTrieInstance();

//    private Map<String, Long> memberScoreMap = new HashMap<>();
    private Map<String, Long> memberScoreMapNew = new HashMap<>();

//    private Set<String> newWordSet = new HashSet<>();

//    private Set<String> inputQuerySet = new HashSet<>();

    private boolean initialReload = true;

    public boolean systemDown = false;

    @Autowired
    private Jedis jedis;

    @PostConstruct
    public void loadStarterDataToTrieAndRedis() {
        if (jedis.dbSize() == 0) {
            trieDataStore.TrieLoadData(wordList.subList(0, dataLoadSize));
            loadDataToRedis();
            initialReload = false;
            trieDataStore.reset();
        }
    }

    public void loadDataToRedis() {
        Map<String, Double> suggestionWithScore = new HashMap<>();
        for (String prefix : trieDataStore.allPrefixes()) {
            List<String> suggestionsList = trieDataStore.suggest(prefix);
            int i = 0;
            for (String suggestion : suggestionsList) {
                suggestionWithScore.put(suggestion, initialReload ? 0d : memberScoreMapNew.get(suggestion));
                i++;
                if (i == redisSetSize / (initialReload ? 1 : redisRetainRatio)) break;
            }
            if (jedis.exists(prefix)) {
                double existingSuggestionListSize = jedis.zcount(prefix, 0, Double.MAX_VALUE);
                if (existingSuggestionListSize + suggestionWithScore.size() > redisSetSize) {
                    double extraElements = (existingSuggestionListSize + suggestionWithScore.size()) - redisSetSize;
                    jedis.zremrangeByRank(prefix, 0, (long) extraElements - 1);  // remove extra elements with the lowest scores
                }
            }
            jedis.zadd(prefix, suggestionWithScore);  // key present then add if final size less or equal to max size
            suggestionWithScore = new HashMap<>();
        }
    }

    public void logInputQuery(String word) {
//        inputQuerySet.add(word);
        if (memberScoreMapNew.containsKey(word)) {
            memberScoreMapNew.put(word, memberScoreMapNew.get(word) + 1);
        } else {
            memberScoreMapNew.put(word, 1l);
        }
    }

//    public void processQuery(String word) {
//        boolean wordAlreadyInCache = jedis.exists(word);
//        List<String> suggestions = jedis.zrange(word, 0, -1); //FIXME: sometimes word can be already present as a prefix even if not encountered before
//            /*
//             eg . if we had earlier processed the word herself , her will be present as a prefix in cache even though it won't contain the word her same for word he
//                need to find a better approach to fix such issues.
//             */
//        if (wordAlreadyInCache && suggestions.contains(word)) {
//            if (memberScoreMap.containsKey(word)) {
//                memberScoreMap.put(word, memberScoreMap.get(word) + 1);
//            } else {
//                memberScoreMap.put(word, 1L);
//            }
//        } else {
////            newWordSet.add(word);
//            trieDataStore.insertWord(word);
//        }
//    }

    public void processQuery2(String word, Long wordScore) {
        boolean wordAlreadyInCacheAsKey = jedis.exists(word);
        List<String> suggestions = jedis.zrange(word, 0, -1); //FIXME: sometimes word can be already present as a prefix even if not encountered before
            /*
             eg . if we had earlier processed the word herself , her will be present as a prefix in cache even though it won't contain the word her same for word he
                need to find a better approach to fix such issues.
             */
        if (!isWordInCache(word, wordAlreadyInCacheAsKey, suggestions)) {
            trieDataStore.insertWord(word);
        } else {
            redisIncreaseScoreOfExistingMembers2(word, wordScore);
//            memberScoreMapNew.remove(word);
        }
    }

    private static boolean isWordInCache(String word, boolean wordAlreadyInCacheAsKey, List<String> suggestions) {
        return wordAlreadyInCacheAsKey && suggestions.contains(word);
    }


    private void redisIncreaseScoreOfExistingMembers2(String word, Long wordScore) {
        List<String> allPrefixesForWord = extractAllPrefixes(word);
        for (String prefix : allPrefixesForWord) {
            long setSize = jedis.zcount(prefix, 0d, Double.MAX_VALUE);
            if (setSize < redisSetSize) {
                jedis.zincrby(prefix, wordScore, word);
            } else {
                jedis.zpopmin(prefix);
                jedis.zincrby(prefix, wordScore, word);
            }
        }

    }

    private List<String> extractAllPrefixes(String word) {
        List<String> prefixes = new ArrayList<>();
        for (int i = 1; i <= word.length(); i++) {
            prefixes.add(word.substring(0, i));
        }
        return prefixes;
    }

    @Scheduled(cron = "*/30 * * * * *")
    public void cleanUpAndDataLoading() {
        try {
            if (memberScoreMapNew.size() >= 100) {
                systemDown = true;
//                for (String word : inputQuerySet) {
//                    processQuery(word);
//                }
                for (Map.Entry<String, Long> entry : memberScoreMapNew.entrySet()) {
                    Long wordScore = entry.getValue();
                    String word = entry.getKey();
                    processQuery2(word, wordScore);
                }
//                inputQuerySet = new HashSet<>();
//                redisIncreaseScoreOfExistingMembers();
//                memberScoreMap = new HashMap<>();
                loadDataToRedis();
                memberScoreMapNew = new HashMap<>();
                trieDataStore.reset();
//                inputQuerySet = new HashSet<>();
                systemDown = false;
            }
        } catch (Exception e) {
            systemDown = false;
            log.error("What the actual fuck {}", e);
        }
    }
}
