package az.whatsapp.controller;

import az.whatsapp.dto.request.CreateUserRequest;
import az.whatsapp.dto.response.UserResponse;
import az.whatsapp.service.UserService;

import jakarta.validation.Valid;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/users")
public class UserController {

    private final UserService users;

    public UserController(UserService users) {
        this.users = users;
    }

    /** Sign-up: open, like registration (no X-User-Id yet). */
    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public UserResponse create(@Valid @RequestBody CreateUserRequest request) {
        return UserResponse.from(users.create(request));
    }

    @GetMapping("/{id}")
    public UserResponse get(@PathVariable long id) {
        return UserResponse.from(users.get(id));
    }
}
