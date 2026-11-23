package az.whatsapp.config;

import az.whatsapp.gateway.GatewayInstance;
import az.whatsapp.gateway.InstanceChannelListener;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

@Configuration
public class RedisConfig {

    /** Every instance listens on its own channel only: frames are sent to the instance holding the connection. */
    @Bean
    public RedisMessageListenerContainer instanceChannelContainer(RedisConnectionFactory connectionFactory,
                                                                  InstanceChannelListener listener,
                                                                  GatewayInstance gateway) {
        var container = new RedisMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.addMessageListener(listener, new ChannelTopic(gateway.channel()));
        return container;
    }
}
