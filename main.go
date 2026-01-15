package main

import (
	"archive/zip"
	"bytes"
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

type Stats struct {
	TotalCount      int `json:"total_count"`      // уровень 3
	DuplicatesCount int `json:"duplicates_count"` // уровень 3
	TotalItems      int `json:"total_items"`
	TotalCategories int `json:"total_categories"`
	TotalPrice      int `json:"total_price"`
}

var db *sql.DB

func main() {
	connStr := os.Getenv("DATABASE_URL")
	if connStr == "" {
		connStr = "host=localhost port=5432 user=validator password=val1dat0r dbname=project-sem-1 sslmode=disable"
	}

	var err error
	db, err = sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("DB open error: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for {
		if err := db.PingContext(ctx); err == nil {
			break
		}
		log.Println("Waiting for postgres...")
		time.Sleep(2 * time.Second)
	}

	// Создаём таблицу ДО запуска HTTP
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS prices (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			category TEXT NOT NULL,
			price INTEGER NOT NULL
		)
	`)
	if err != nil {
		log.Fatalf("Table create error: %v", err)
	}

	http.HandleFunc("/api/v0/prices", pricesHandler)
	http.HandleFunc("/api/v0/prices/", pricesHandler)

	log.Println("Listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func pricesHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		handlePost(w, r)
	case http.MethodGet:
		handleGet(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func handlePost(w http.ResponseWriter, r *http.Request) {
	log.Printf("[POST] Content-Type: %q | Query: %s", r.Header.Get("Content-Type"), r.URL.RawQuery)

	if err := r.ParseMultipartForm(10 << 20); err != nil {
		log.Printf("[ERROR] Multipart error: %v", err)
		http.Error(w, "multipart error", http.StatusBadRequest)
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		log.Printf("[ERROR] No file field: %v", err)
		http.Error(w, "file missing", http.StatusBadRequest)
		return
	}
	defer file.Close()

	body, err := io.ReadAll(file)
	if err != nil {
		http.Error(w, "read error", http.StatusBadRequest)
		return
	}

	zr, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		http.Error(w, "invalid zip", http.StatusBadRequest)
		return
	}

	_, _ = db.Exec("TRUNCATE TABLE prices")

	totalItems := 0
	totalPrice := 0
	categories := map[string]bool{}
	found := false

	for _, f := range zr.File {
		if !strings.HasSuffix(f.Name, ".csv") {
			continue
		}
		found = true

		rc, err := f.Open()
		if err != nil {
			continue
		}
		defer rc.Close()

		reader := csv.NewReader(rc)
		reader.Comma = ','
		reader.LazyQuotes = true

		// Пропуск первой строки (заголовок или данные)
		_, _ = reader.Read() // игнорируем ошибку, если нет заголовка

		for {
			row, err := reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				continue
			}

			if len(row) < 4 {
				continue
			}

			name := strings.TrimSpace(row[1])
			category := strings.TrimSpace(row[2])
			priceStr := strings.TrimSpace(row[3])

			price, err := strconv.Atoi(priceStr)
			if err != nil {
				continue
			}

			_, err = db.Exec(
				"INSERT INTO prices(name, category, price) VALUES ($1,$2,$3)",
				name, category, price,
			)
			if err != nil {
				http.Error(w, "db error", http.StatusInternalServerError)
				return
			}

			totalItems++
			totalPrice += price
			categories[category] = true
		}
	}

	if !found {
		http.Error(w, "csv not found", http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(Stats{
		TotalCount:      totalItems,
		DuplicatesCount: 0,
		TotalItems:      totalItems,
		TotalCategories: len(categories),
		TotalPrice:      totalPrice,
	})
}

func handleGet(w http.ResponseWriter, r *http.Request) {
	rows, err := db.Query("SELECT name, category, price FROM prices ORDER BY id")
	if err != nil {
		http.Error(w, "db query error", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var buf bytes.Buffer
	zipWriter := zip.NewWriter(&buf)
	csvFile, _ := zipWriter.Create("data.csv")
	csvWriter := csv.NewWriter(csvFile)
	csvWriter.Write([]string{"name", "category", "price"})

	for rows.Next() {
		var n, c string
		var p int
		if err := rows.Scan(&n, &c, &p); err != nil {
			continue
		}
		csvWriter.Write([]string{n, c, strconv.Itoa(p)})
	}

	csvWriter.Flush()
	zipWriter.Close()

	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", "attachment; filename=data.zip")
	w.Write(buf.Bytes())
}
