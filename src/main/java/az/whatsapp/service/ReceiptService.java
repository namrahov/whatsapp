package az.whatsapp.service;

import az.whatsapp.repository.ReceiptRepository;

import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

@Service
public class ReceiptService {

    private final ReceiptRepository receipts;

    public ReceiptService(ReceiptRepository receipts) {
        this.receipts = receipts;
    }

    /** DELIVERED: the recipient's device has stored the message locally. */
    public void markDelivered(long userId, long messageId) {
        if (!receipts.markDelivered(messageId, userId)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Message not found");
        }
    }

    /** READ: the recipient has opened the message. */
    public void markRead(long userId, long messageId) {
        if (!receipts.markRead(messageId, userId)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Message not found");
        }
    }
}
