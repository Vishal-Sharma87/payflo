package com.vishal.payflo.configs;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

@Configuration
public class RedisConfigs {

    private final RedisConnectionProperties redisConnectionProperties;

    public RedisConfigs(RedisConnectionProperties redisConnectionProperties){
        this.redisConnectionProperties = redisConnectionProperties;
    }


    @Bean
    public StringRedisTemplate redisTemplate(RedisConnectionFactory redisConnectionFactory){
        return new StringRedisTemplate(redisConnectionFactory);
    }

    @Bean
    public RedisConnectionFactory redisConnectionFactory(){
        RedisStandaloneConfiguration redisStandaloneConfiguration = new RedisStandaloneConfiguration();
        redisStandaloneConfiguration.setHostName(redisConnectionProperties.host());
        redisStandaloneConfiguration.setPort(redisConnectionProperties.port());

        return  new LettuceConnectionFactory(redisStandaloneConfiguration);
    }


}
