package az.whatsapp.service;

import az.whatsapp.dto.request.CreateUserRequest;
import az.whatsapp.entity.UserEntity;
import az.whatsapp.repository.UserRepository;

import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

@Service
public class UserService {

    private final UserRepository users;

    public UserService(UserRepository users) {
        this.users = users;
    }

    public UserEntity create(CreateUserRequest request) {
        try {
            return users.saveAndFlush(new UserEntity(request.username(), request.displayName().strip()));
        } catch (DataIntegrityViolationException e) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Username is already taken");
        }
    }

    public UserEntity get(long id) {
        return users.findById(id)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "User not found"));
    }
}
