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
	if err := r.ParseMultipartForm(10 << 20); err != nil {
		http.Error(w, "Bad multipart", http.StatusBadRequest)
		return
	}

	file, _, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "File missing", http.StatusBadRequest)
		return
	}
	defer file.Close()

	body, _ := io.ReadAll(file)

	zipReader, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		http.Error(w, "Invalid zip", http.StatusBadRequest)
		return
	}

	db.Exec("TRUNCATE TABLE prices")

	totalItems := 0
	totalPrice := 0
	categories := map[string]bool{}
	found := false

	for _, zf := range zipReader.File {
		if !strings.HasSuffix(zf.Name, ".csv") {
			continue
		}
		found = true

		f, _ := zf.Open()
		defer f.Close()

		reader := csv.NewReader(f)

		// skip header
		reader.Read()

		for {
			row, err := reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				http.Error(w, "CSV error", http.StatusBadRequest)
				return
			}

			// id,name,category,price,create_date
			if len(row) != 5 {
				http.Error(w, "Bad CSV format", http.StatusBadRequest)
				return
			}

			name := row[1]
			category := row[2]
			price, err := strconv.Atoi(row[3])
			if err != nil {
				http.Error(w, "Bad price", http.StatusBadRequest)
				return
			}

			_, err = db.Exec(
				"INSERT INTO prices (name, category, price) VALUES ($1,$2,$3)",
				name, category, price,
			)
			if err != nil {
				http.Error(w, "DB insert error", http.StatusInternalServerError)
				return
			}

			totalItems++
			totalPrice += price
			categories[category] = true
		}
	}

	if !found {
		http.Error(w, "CSV not found", http.StatusBadRequest)
		return
	}

	resp := Stats{
		TotalItems:      totalItems,
		TotalCategories: len(categories),
		TotalPrice:      totalPrice,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func handleGet(w http.ResponseWriter, r *http.Request) {
	var buf bytes.Buffer
	zipWriter := zip.NewWriter(&buf)

	csvFile, _ := zipWriter.Create("data.csv")
	csvWriter := csv.NewWriter(csvFile)
	csvWriter.Write([]string{"name", "category", "price"})

	rows, err := db.Query("SELECT name, category, price FROM prices ORDER BY id")
	if err == nil {
		for rows.Next() {
			var n, c string
			var p int
			rows.Scan(&n, &c, &p)
			csvWriter.Write([]string{n, c, strconv.Itoa(p)})
		}
		rows.Close()
	}

	csvWriter.Flush()
	zipWriter.Close()

	w.Header().Set("Content-Type", "application/zip")
	w.WriteHeader(http.StatusOK)
	w.Write(buf.Bytes())
}
