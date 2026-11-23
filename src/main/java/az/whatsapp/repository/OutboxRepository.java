package az.whatsapp.repository;

import az.whatsapp.entity.OutboxEventEntity;

import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;

public interface OutboxRepository extends JpaRepository<OutboxEventEntity, UUID> {
}
