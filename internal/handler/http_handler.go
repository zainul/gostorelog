package handler

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"

	"gostorelog/internal/entity"
	"gostorelog/internal/usecase"
)

// PanicRecoveryMiddleware recovers from panics in HTTP handlers
func PanicRecoveryMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				log.Printf("Panic recovered in HTTP handler: %v", err)
				http.Error(w, "Internal server error", http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// HTTPHandler handles HTTP requests
type HTTPHandler struct {
	usecase usecase.StorageUsecase
}

// NewHTTPHandler creates a new HTTP handler
func NewHTTPHandler(usecase usecase.StorageUsecase) *HTTPHandler {
	return &HTTPHandler{
		usecase: usecase,
	}
}

// Publish handles POST /publish
func (h *HTTPHandler) Publish(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		Data         interface{} `json:"data"`
		DataType     int         `json:"data_type"`
		PartitionKey string      `json:"partition_key"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := h.usecase.StoreRecord(req.Data, entity.DataType(req.DataType), req.PartitionKey); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// Read handles GET /read?partition=<key>&offset=<offset>
func (h *HTTPHandler) Read(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	partition := r.URL.Query().Get("partition")
	offsetStr := r.URL.Query().Get("offset")
	offset, err := strconv.ParseUint(offsetStr, 10, 64)
	if err != nil {
		http.Error(w, "Invalid offset", http.StatusBadRequest)
		return
	}
	record, err := h.usecase.RetrieveRecord(partition, offset)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(record)
}

// Replicate handles POST /replicate for receiving replicated data
func (h *HTTPHandler) Replicate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		Data         interface{} `json:"data"`
		DataType     int         `json:"data_type"`
		PartitionKey string      `json:"partition_key"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// For replication, store the data as received, disable replication to avoid loop
	// Temporarily unset replicator
	impl := h.usecase.(*usecase.StorageUsecaseImpl)
	replicator := impl.Replicator
	impl.Replicator = nil
	err := h.usecase.StoreRecord(req.Data, entity.DataType(req.DataType), req.PartitionKey)
	impl.Replicator = replicator
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "replicated"})
}

// Status handles GET /status for reporting node status
func (h *HTTPHandler) Status(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	// For now, return a simple status; in future, return last offsets per partition
	status := map[string]interface{}{
		"node": "follower", // or leader, but for now assume follower
		"last_partition": "test-partition", // placeholder
	}
	json.NewEncoder(w).Encode(status)
}

// Gaps handles GET /gaps for querying gap information
func (h *HTTPHandler) Gaps(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	// For now, return placeholder gaps
	gaps := []map[string]interface{}{
		{"node": "follower1", "gap": 5, "partition": "test-partition"},
	}
	json.NewEncoder(w).Encode(gaps)
}

// GetMux returns the HTTP mux for testing
func (h *HTTPHandler) GetMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/publish", h.Publish)
	mux.HandleFunc("/read", h.Read)
	mux.HandleFunc("/replicate", h.Replicate)
	mux.HandleFunc("/status", h.Status)
	mux.HandleFunc("/gaps", h.Gaps)
	return mux
}

// StartServer starts the HTTP server
func (h *HTTPHandler) StartServer(addr string) *http.Server {
	mux := h.GetMux()
	server := &http.Server{
		Addr:    addr,
		Handler: PanicRecoveryMiddleware(mux),
	}
	go func() {
		log.Printf("Starting HTTP server on %s", addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()
	return server
}