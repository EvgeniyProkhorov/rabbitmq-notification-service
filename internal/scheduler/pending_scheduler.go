package scheduler

import (
	"context"
	"log"
	"time"
)

type PendingProcessor interface {
	ProcessPendingEvents(ctx context.Context) error
}

// PendingScheduler периодически запускает обработку pending-событий.
type PendingScheduler struct {
	processor PendingProcessor
	interval  time.Duration
}

// NewPendingScheduler создаёт scheduler для периодической обработки pending-событий.
func NewPendingScheduler(
	processor PendingProcessor,
	interval time.Duration,
) *PendingScheduler {
	return &PendingScheduler{
		processor: processor,
		interval:  interval,
	}
}

// Start запускает периодическую обработку pending-событий до отмены context.
func (s *PendingScheduler) Start(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("pending scheduler stopped: %v", ctx.Err())
			return

		case <-ticker.C:
			if err := s.processor.ProcessPendingEvents(ctx); err != nil {
				log.Printf("process pending events: %v", err)
			}
		}
	}
}
