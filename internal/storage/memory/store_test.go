package memory_test

import (
	"testing"

	"github.com/deshenrao/rabbitmq-lite/internal/storage"
	"github.com/deshenrao/rabbitmq-lite/internal/storage/memory"
	"github.com/deshenrao/rabbitmq-lite/internal/storage/storagetest"
)

func TestMemoryStoreConformance(t *testing.T) {
	storagetest.Run(t, func(t *testing.T) storage.Store {
		return memory.New()
	})
}
