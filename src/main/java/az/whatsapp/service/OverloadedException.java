package az.whatsapp.service;

/** The request was not run because a capacity limit is reached; answered with 503 and this Retry-After. */
public class OverloadedException extends RuntimeException {

    private final long retryAfterSeconds;

    public OverloadedException(String message, long retryAfterSeconds) {
        super(message);
        this.retryAfterSeconds = retryAfterSeconds;
    }

    public long retryAfterSeconds() {
        return retryAfterSeconds;
    }
}
