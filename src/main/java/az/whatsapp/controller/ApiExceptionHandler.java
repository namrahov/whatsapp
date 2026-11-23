package az.whatsapp.controller;

import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * PostgreSQL unreachable: nothing was stored, so answer 503 + Retry-After instead of a generic 500.
 * The client keeps the message in its local outbox and retries with the same clientMessageId.
 */
@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler({CannotCreateTransactionException.class, DataAccessResourceFailureException.class,
            TransientDataAccessException.class})
    public ResponseEntity<ProblemDetail> storageUnavailable() {
        var problem = ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE,
                "Storage temporarily unavailable, not accepted. Retry with the same clientMessageId.");
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .header(HttpHeaders.RETRY_AFTER, "2")
                .body(problem);
    }
}
