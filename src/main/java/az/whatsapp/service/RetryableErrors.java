package az.whatsapp.service;

import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.transaction.CannotCreateTransactionException;

/** Failures where nothing was stored and the same request may succeed later (e.g. PostgreSQL unreachable). */
public final class RetryableErrors {

    private RetryableErrors() {
    }

    public static boolean isRetryable(Throwable e) {
        return e instanceof CannotCreateTransactionException
                || e instanceof DataAccessResourceFailureException
                || e instanceof TransientDataAccessException;
    }
}
