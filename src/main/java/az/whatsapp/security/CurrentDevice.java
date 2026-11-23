package az.whatsapp.security;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Injects the calling device's id into a controller method parameter of type {@code String}.
 * With {@code required = false} the parameter is {@code null} when the request names no device.
 */
@Target(ElementType.PARAMETER)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface CurrentDevice {

    boolean required() default true;
}
