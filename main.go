package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/mail"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Submission represents the form payload
type Submission struct {
	FormID         string                 `json:"form_id"`
	IdempotencyKey string                 `json:"idempotency_key"`
	Email          string                 `json:"email"`
	Data           map[string]interface{} `json:"data"`
	SubmittedAt    time.Time              `json:"submitted_at"`
}

// MemoryDB simulates our database with strict concurrency protection
type MemoryDB struct {
	mu           sync.RWMutex
	submissions  []Submission
	idempotency  map[string]Submission
}

var db = &MemoryDB{
	submissions: []Submission{},
	idempotency: make(map[string]Submission),
}

func main() {
	http.HandleFunc("/submissions", handlePostSubmissions)
	http.HandleFunc("/forms/", handleGetSubmissions)

	fmt.Println("Server running smoothly on :8080...")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		fmt.Println("Server error:", err)
	}
}

// Part 1: POST /submissions
func handlePostSubmissions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var sub Submission
	if err := json.NewDecoder(r.Body).Decode(&sub); err != nil {
		http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
		return
	}

	// 1. Validation Logic
	if strings.TrimSpace(sub.FormID) == "" {
		http.Error(w, "Missing form_id", http.StatusBadRequest)
		return
	}
	if _, err := mail.ParseAddress(sub.Email); err != nil {
		http.Error(w, "Invalid email address", http.StatusBadRequest)
		return
	}

	// 2. Duplicate Protection (Idempotency check)
	db.mu.Lock()
	defer db.mu.Unlock()

	if existing, exists := db.idempotency[sub.IdempotencyKey]; exists {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK) // Return existing record seamlessly
		json.NewEncoder(w).Encode(existing)
		return
	}

	// 3. Store Submission
	sub.SubmittedAt = time.Now()
	db.submissions = append(db.submissions, sub)
	db.idempotency[sub.IdempotencyKey] = sub

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(sub)
}

// Part 1: GET /forms/{form_id}/submissions with Pagination
func handleGetSubmissions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract form_id from path URL: /forms/{form_id}/submissions
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 3 || parts[0] != "forms" || parts[2] != "submissions" {
		http.Error(w, "Not Found", http.StatusNotFound)
		return
	}
	formID := parts[1]

	// Parse pagination parameters
	query := r.URL.Query()
	page, _ := strconv.Atoi(query.Get("page"))
	if page < 1 {
		page = 1
	}
	pageSize := 10

	db.mu.RLock()
	defer db.mu.RUnlock()

	// Filter array items matching formID
	var filtered []Submission
	for _, s := range db.submissions {
		if s.FormID == formID {
			filtered = append(filtered, s)
		}
	}

	// Perform slicing pagination offsets
	total := len(filtered)
	start := (page - 1) * pageSize
	end := start + pageSize

	if start > total {
		start = total
	}
	if end > total {
		end = total
	}

	paginatedResult := filtered[start:end]

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"page":        page,
		"per_page":    pageSize,
		"total_items": total,
		"data":        paginatedResult,
	})
}
