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
	defer r.Body.Close()

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read error", 400)
		return
	}

	zr, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		http.Error(w, "invalid zip", 400)
		return
	}

	db.Exec("TRUNCATE TABLE prices")

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

		reader := csv.NewReader(rc)
		reader.Read() // skip header

		for {
			row, err := reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				rc.Close()
				http.Error(w, "csv error", 400)
				return
			}

			name := row[1]
			category := row[2]
			price, _ := strconv.Atoi(row[3])

			_, err = db.Exec(
				"INSERT INTO prices(name, category, price) VALUES ($1,$2,$3)",
				name, category, price,
			)
			if err != nil {
				rc.Close()
				http.Error(w, "db error", 500)
				return
			}

			totalItems++
			totalPrice += price
			categories[category] = true
		}

		rc.Close()
	}

	if !found {
		http.Error(w, "csv not found", 400)
		return
	}

	json.NewEncoder(w).Encode(Stats{
		TotalItems:      totalItems,
		TotalCategories: len(categories),
		TotalPrice:      totalPrice,
	})
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
