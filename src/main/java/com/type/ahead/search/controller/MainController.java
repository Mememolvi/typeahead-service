package com.type.ahead.search.controller;

import com.type.ahead.search.service.JedisService;
import com.type.ahead.search.util.TrieDataStore;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import redis.clients.jedis.Jedis;

import java.util.*;

@RestController
@RequestMapping("/prefix-search")
@Slf4j
public class MainController {
    @Value("${common.word.list}")
    List<String> wordList;

    @Value("${redis.sorted.set.size}")
    Integer redisSetSize;

    @Value("${redis.retain.ratio}")
    Integer redisRetainRatio;

    private final TrieDataStore trieDataStore = TrieDataStore.getTrieInstance();
    @Autowired
    private Jedis jedis;

    private Map<String, Long> memberScoreMap = new HashMap<>();
    private Set<String> newWordSet = new HashSet<>();

    @Autowired
    private JedisService jedisService;

    @CrossOrigin
    @GetMapping("/test/logQuery/{start}/{end}")
    public ResponseEntity<?> logQuery(@PathVariable int start, @PathVariable int end) {
        try {
            for (String word : wordList.subList(start, end))
                if (!jedisService.systemDown) {
                    jedisService.logInputQuery(word);
                } else {
                    log.warn("Query not processed system is down {}", word);
                }
            return ResponseEntity.status(HttpStatus.OK).body("SUCCESS" + "\n");
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error");
        }
    }


    @CrossOrigin
    @GetMapping("/test/runScenario/{number}")
    public ResponseEntity<?> runScenarioNumber(@PathVariable int number) {
        try {
            log.info("send 1000 words as query's");
            trieDataStore.TrieLoadData(wordList.subList(0, 15000)); // load first thousands words into a trie

            log.info("total nodes in the trie after loading words {}", TrieDataStore.getTrieSize());
            loadWordsToRedis(); // load same words to redis
            log.info("reset trie");
            trieDataStore.reset(); // reset the trie freeing up memory

            log.info("total nodes in the trie after reset {}", TrieDataStore.getTrieSize());

            log.info("sending 150 repeated words");
            for (int i = 0; i < 10; i++) {
                for (String word : wordList.subList(0, 5000)) {
                    // send first 100 words of the wordList as query's since the words are already present in cache
                    // should not be added to the trie
                    processQuery(word);
                }
                for (String word : wordList.subList(0, 2500)) { // HashMap count check
                    // send first 50 words of the wordList as query's since the words are already present in cache
                    // should not be added to the trie
                    processQuery(word);
                }
            }

            log.info("size of already existing member score map {}", memberScoreMap.size());
            log.info("size of new words set {}", newWordSet.size());
            log.info("total nodes in the trie after adding 150 duplicate Nodes {}", TrieDataStore.getTrieSize());
            for (String word : wordList.subList(15000, 20000)) { // HashMap count check
                // send 1000 new words
                processQuery(word);
            }
            log.info("size of new words set {}", newWordSet.size());
            log.info("total nodes in the trie {}", TrieDataStore.getTrieSize());

            redisIncreaseScoreOfExistingMembers(memberScoreMap);
//            trieDataStore.setAllWords(newWordSet);
            loadWordsToRedis();
            for (String word : wordList.subList(15000, 20000)) { // HashMap count check
                // send first 50 words of the wordList as query's since the words are already present in cache
                // should not be added to the trie
                processQuery(word);
            }
            redisIncreaseScoreOfExistingMembers(memberScoreMap);
            // load map of existing words into redis by increasing score
            jedis.flushDB(); // reset cache

            return ResponseEntity.status(HttpStatus.OK).body("SUCCESS" + "\n");
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error");
        }
    }

    private void redisIncreaseScoreOfExistingMembers(Map<String, Long> memberScoreMap) {
        for (Map.Entry<String, Long> member : memberScoreMap.entrySet()) {
            String word = member.getKey();
            Long wordScore = member.getValue();
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
    }


    @CrossOrigin
    @GetMapping("/loadWords/trie/{size}")
    public ResponseEntity<?> loadWordsToTrie(@PathVariable int size) {
        try {
            trieDataStore.TrieLoadData(wordList.subList(0, size));
            return ResponseEntity.status(HttpStatus.OK).body("SUCCESS" + "\n");

        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error");
        }
    }

    @CrossOrigin
    @GetMapping("/query/{word}")
    public ResponseEntity<?> processQuery(@PathVariable String word) {
        try {
            boolean wordAlreadyInCache = jedis.exists(word);
            List<String> suggestions = jedis.zrange(word, 0, -1); //FIXME: sometimes word can be already present as a prefix even if not encountered before
            /*
             eg . if we had earlier processed the word herself , her will be present as a prefix in cache even though it won't contain the word her same for word he
                need to find a better approach to fix such issues.
             */
            if (wordAlreadyInCache && suggestions.contains(word)) {
                if (memberScoreMap.containsKey(word)) {
                    memberScoreMap.put(word, memberScoreMap.get(word) + 1);
                } else {
                    memberScoreMap.put(word, 1L);
                }
            } else {
                newWordSet.add(word);
                trieDataStore.insertWord(word);
            }
            return ResponseEntity.status(HttpStatus.OK).body("SUCCESS" + "\n");
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error");
        }
    }

    @CrossOrigin
    @GetMapping("/loadWords/redis/")
    public ResponseEntity<?> loadWordsToRedis() {
        try {
            Map<String, Double> suggestionWithScore = new HashMap<>();
            for (String prefix : trieDataStore.getAllTriePrefixes()) {
//                jedis.set("key", "value");
//                suggestionMap.put(prefix, trieDataStore.suggest(prefix));
                List<String> suggestionsList = trieDataStore.suggest(prefix);
                int i = 0;
                for (String suggestion : suggestionsList) {
                    suggestionWithScore.put(suggestion, 0d);
                    i++;
                    if (i == redisSetSize / redisRetainRatio) break;
                }
//                String[] suggestionsArray = suggestionsList.toArray(new String[suggestionsList.size()]);
//                jedis.lpush(prefix, suggestionsArray);
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
            log.info(jedis.ping());
            return ResponseEntity.status(HttpStatus.OK).body("SUCCESS" + "\n");

        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error");
        }
    }

    @CrossOrigin
    @GetMapping("/getAll/{prefix}")
    public ResponseEntity<?> getMatches(@PathVariable String prefix) {
        try {
//            trieDataStore.TrieLoadData(words);
            log.info("trie datastore size :{}", TrieDataStore.getTrieSize());
            trieDataStore.getAllTriePrefixes();
            return ResponseEntity.status(HttpStatus.OK).body(trieDataStore.suggest(prefix) + "\n");
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error");
        }
    }

    @CrossOrigin
    @GetMapping("/getAll/redis/{prefix}")
    public ResponseEntity<?> getMatchesFromRedis(@PathVariable String prefix) {
        try {
            if (!jedisService.systemDown)
                return ResponseEntity.status(HttpStatus.OK).body(jedis.zrevrangeWithScores(prefix, 0, -1) + "\n");
            else
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("System down!");

        } catch (Exception e) {
            log.error("", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error");
        }
    }

    @CrossOrigin
    @GetMapping("/getAll")
    public ResponseEntity<?> getAllSuggestionForAllPrefixes() {
        try {
            Map<String, List<String>> suggestionMap = new HashMap<>();
            for (String prefix : trieDataStore.getAllTriePrefixes()) {
                suggestionMap.put(prefix, trieDataStore.suggest(prefix));
            }
            return ResponseEntity.status(HttpStatus.OK).body(suggestionMap + "\n");
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error");
        }
    }

    private List<String> extractAllPrefixes(String word) {
        List<String> prefixes = new ArrayList<>();
        for (int i = 1; i <= word.length(); i++) {
            prefixes.add(word.substring(0, i));
        }
        return prefixes;
    }

}
