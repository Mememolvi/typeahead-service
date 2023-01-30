package com.type.ahead.search.service;


import com.type.ahead.search.util.TrieDataStore;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    // A map to keep track of all the input queries with score (score represents how many times each query was passed)
    private Map<String, Long> queryScoreMap = new HashMap<>();

    private boolean initialReload = true;

    public boolean systemDown = false;

    @Autowired
    private Jedis jedis;

    /**
     * This method is responsible for the initial setup needed for the service to work
     * When we start the service both the trieDataStore and the Redis data store are empty
     * so won't be able to provide any suggestions, to counter this issue we create a set of
     * words that are most relevant to us and load them first to the trieDataStore and there
     * to the redis in memory cache thus allowing us to provide suggestions just after the
     * initial service start up.
     * Here are I am directly this starter set of words from application.properties file
     * based on our needs we can change that and read from some database instead.
     */
    @PostConstruct
    public void loadStarterDataToTrieAndRedis() {
        // Checking DB size if redis is not empty we don't need to reload the data at each startup
        if (jedis.dbSize() == 0) {
            trieDataStore.TrieLoadData(wordList.subList(0, dataLoadSize));
            loadDataToRedis();
            trieDataStore.reset();
        }
        initialReload = false;
    }

    /**
     * Method used to load data to redis cache this method gets called initially when the application start
     * and then again periodically as a cron job.
     */
    public void loadDataToRedis() {
        Map<String, Double> prefixToSuggestionListWithScoreMap = new HashMap<>();
        // Get all possible prefixes of all the words stored in the TrieDataStore and Iterate over them
        for (String prefix : trieDataStore.getAllTriePrefixes()) {
            // Get all suggestions for a given prefix
            List<String> suggestionsList = trieDataStore.suggest(prefix);
            int i = 0;
            for (String suggestion : suggestionsList) {
                prefixToSuggestionListWithScoreMap.put(suggestion, initialReload ? 0d : queryScoreMap.get(suggestion)); // during initial reload set score of each suggestion to
                i++;                                                                                                    // Other wise get score from queryScoreMap
                if (i == redisSetSize / (initialReload ? 1 : redisRetainRatio)) break;
            }
            if (jedis.exists(prefix)) {
                double existingSuggestionListSize = jedis.zcount(prefix, 0, Double.MAX_VALUE);
                if (existingSuggestionListSize + prefixToSuggestionListWithScoreMap.size() > redisSetSize) {
                    double extraElements = (existingSuggestionListSize + prefixToSuggestionListWithScoreMap.size()) - redisSetSize;
                    jedis.zremrangeByRank(prefix, 0, (long) extraElements - 1);  // remove extra elements with the lowest scores
                }
            }
            jedis.zadd(prefix, prefixToSuggestionListWithScoreMap);  // key present then add if final size less or equal to max size
            prefixToSuggestionListWithScoreMap = new HashMap<>();
        }
    }

    public void logInputQuery(String word) {
        if (queryScoreMap.containsKey(word)) {
            queryScoreMap.put(word, queryScoreMap.get(word) + 1);
        } else {
            queryScoreMap.put(word, 1l);
        }
    }

    public void processQuery(String word, Long wordScore) {
        boolean wordAlreadyInCacheAsKey = jedis.exists(word);
        List<String> suggestions = jedis.zrange(word, 0, -1); //FIXME: sometimes word can be already present as a prefix even if not encountered before
            /*
             eg . if we had earlier processed the word herself , her will be present as a prefix in cache even though it won't contain the word her same for word he
                need to find a better approach to fix such issues.
             */
        if (!isWordInCache(word, wordAlreadyInCacheAsKey, suggestions)) {
            trieDataStore.insertWord(word);
        } else {
            redisIncreaseScoreOfExistingMembers(word, wordScore);
//            memberScoreMapNew.remove(word);
        }
    }

    private static boolean isWordInCache(String word, boolean wordAlreadyInCacheAsKey, List<String> suggestions) {
        return wordAlreadyInCacheAsKey && suggestions.contains(word);
    }


    private void redisIncreaseScoreOfExistingMembers(String word, Long wordScore) {
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
            if (queryScoreMap.size() >= 5) {
                systemDown = true;
                for (Map.Entry<String, Long> entry : queryScoreMap.entrySet()) {
                    Long wordScore = entry.getValue();
                    String word = entry.getKey();
                    processQuery(word, wordScore);
                }
                loadDataToRedis();
                queryScoreMap = new HashMap<>();
                trieDataStore.reset();
                systemDown = false;
            }
        } catch (Exception e) {
            systemDown = false;
            log.error("What the actual fuck {}", e);
        }
    }
}
