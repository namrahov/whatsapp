package az.whatsapp.config;

import az.whatsapp.security.CurrentDeviceArgumentResolver;
import az.whatsapp.security.CurrentUserArgumentResolver;

import java.util.List;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.method.support.HandlerMethodArgumentResolver;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class WebConfig implements WebMvcConfigurer {

    private final CurrentUserArgumentResolver currentUserArgumentResolver;
    private final CurrentDeviceArgumentResolver currentDeviceArgumentResolver;

    public WebConfig(CurrentUserArgumentResolver currentUserArgumentResolver,
                     CurrentDeviceArgumentResolver currentDeviceArgumentResolver) {
        this.currentUserArgumentResolver = currentUserArgumentResolver;
        this.currentDeviceArgumentResolver = currentDeviceArgumentResolver;
    }

    @Override
    public void addArgumentResolvers(List<HandlerMethodArgumentResolver> resolvers) {
        resolvers.add(currentUserArgumentResolver);
        resolvers.add(currentDeviceArgumentResolver);
    }
}
