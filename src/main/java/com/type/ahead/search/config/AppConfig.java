package com.type.ahead.search.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import redis.clients.jedis.Jedis;

@Configuration
@EnableScheduling
public class AppConfig {
    @Bean
    public Jedis jedis() {
        return new Jedis();
    }

}
