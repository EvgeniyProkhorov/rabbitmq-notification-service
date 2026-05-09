package main

import (
	"context"
	"database/sql"
	"log"
	"net/http"
	"os"
	"rabbitmq-notification-service/internal/broker"
	"rabbitmq-notification-service/internal/handler"
	"rabbitmq-notification-service/internal/repository"
	"rabbitmq-notification-service/internal/scheduler"
	"rabbitmq-notification-service/internal/service"
	"rabbitmq-notification-service/internal/validation"
	"time"

	_ "github.com/lib/pq"
)

// type noopPublisher struct{}

// func (p *noopPublisher) PublishEvent(ctx context.Context, event domain.Event) error {
// 	return nil
// }

func main() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Fatal("DATABASE_URL is required")
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatalf("open database: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("ping database: %v", err)
	}

	eventRepo := repository.NewPostgresEventRepository(db)

	rabbitURL := os.Getenv("RABBITMQ_URL")
	if rabbitURL == "" {
		log.Fatal("RABBITMQ_URL is required")
	}

	ctx := context.Background()

	rabbitPublisher, err := broker.NewRabbitPublisher(ctx, rabbitURL)
	if err != nil {
		log.Fatalf("create rabbit publisher: %v", err)
	}

	eventService := service.NewEventService(eventRepo, rabbitPublisher)

	rabbitConsumer, err := broker.NewRabbitConsumer(ctx, rabbitURL, eventService)
	if err != nil {
		log.Fatalf("create rabbit consumer: %v", err)
	}

	pendingScheduler := scheduler.NewPendingScheduler(eventService, 30*time.Second)
	go pendingScheduler.Start(ctx)

	go func() {
		if err := rabbitConsumer.Start(ctx); err != nil {
			log.Printf("rabbit consumer stopped: %v", err)
		}
	}()

	validate := validation.NewValidator()

	eventHandler := handler.NewEventHandler(eventService, validate)

	router := handler.NewRouter(eventHandler)

	addr := ":8080"

	log.Printf("server listening on %s", addr)

	if err := http.ListenAndServe(addr, router); err != nil {
		log.Fatalf("listen and serve: %v", err)
	}
}
