package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"rabbitmq-notification-service/internal/broker"
	"rabbitmq-notification-service/internal/handler"
	"rabbitmq-notification-service/internal/repository"
	"rabbitmq-notification-service/internal/scheduler"
	"rabbitmq-notification-service/internal/service"
	"rabbitmq-notification-service/internal/validation"
	"sort"
	"syscall"
	"time"

	_ "github.com/lib/pq"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

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

	if err := runMigrations(ctx, db, "migrations"); err != nil {
		log.Fatalf("run migrations: %v", err)
	}

	eventRepo := repository.NewPostgresEventRepository(db)

	rabbitURL := os.Getenv("RABBITMQ_URL")
	if rabbitURL == "" {
		log.Fatal("RABBITMQ_URL is required")
	}

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

	server := &http.Server{
		Addr:    ":8080",
		Handler: router,
	}

	go func() {
		log.Printf("server listening on %s", server.Addr)

		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("listen and serve: %v", err)
		}
	}()

	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("shutdown server: %v", err)
	}

	log.Println("server stopped")
}

func runMigrations(ctx context.Context, db *sql.DB, dir string) error {
	files, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("read migrations dir: %w", err)
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i].Name() < files[j].Name()
	})

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		if filepath.Ext(file.Name()) != ".sql" {
			continue
		}

		path := filepath.Join(dir, file.Name())

		query, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", file.Name(), err)
		}

		if _, err := db.ExecContext(ctx, string(query)); err != nil {
			return fmt.Errorf("execute migration %s: %w", file.Name(), err)
		}

		log.Printf("migration applied: %s", file.Name())
	}

	return nil
}
