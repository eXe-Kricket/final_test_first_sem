package main

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
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
	TotalCount      int     `json:"total_count"`
	DuplicatesCount int     `json:"duplicates_count"`
	TotalItems      int     `json:"total_items"`
	TotalCategories int     `json:"total_categories"`
	TotalPrice      float64 `json:"total_price"`
}

type rowData struct {
	Name     string
	Category string
	Price    float64
	Date     time.Time
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
		panic(err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	for {
		if err := db.PingContext(ctx); err == nil {
			break
		}
		log.Println("Waiting for PostgreSQL...")
		time.Sleep(2 * time.Second)
	}

	http.HandleFunc("/api/v0/prices", pricesHandler)

	if err := http.ListenAndServe(":8080", nil); err != nil {
		panic(err)
	}
}

/* ROUTER */

func pricesHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		handlePost(w, r)
	case http.MethodGet:
		handleGet(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

/* POST */

func handlePost(w http.ResponseWriter, r *http.Request) {
	queryType := r.URL.Query().Get("type")

	if err := r.ParseMultipartForm(10 << 20); err != nil {
		http.Error(w, "multipart error", http.StatusBadRequest)
		return
	}

	file, _, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "file missing", http.StatusBadRequest)
		return
	}
	defer file.Close()

	body, err := io.ReadAll(file)
	if err != nil {
		http.Error(w, "read error", http.StatusBadRequest)
		return
	}

	tx, err := db.Begin()
	if err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}
	defer tx.Rollback()

	totalCount := 0
	var rows []rowData

	switch queryType {
	case "", "zip":
		rows, err = readZip(body, &totalCount)
	case "tar":
		rows, err = readTar(body, &totalCount)
	default:
		http.Error(w, "unsupported archive type", http.StatusBadRequest)
		return
	}

	if err != nil {
		http.Error(w, "archive error", http.StatusBadRequest)
		return
	}

	inserted := 0
	duplicates := 0

	for _, r := range rows {
		res, err := tx.Exec(`
			INSERT INTO prices (name, category, price, create_date)
			VALUES ($1,$2,$3,$4)
			ON CONFLICT DO NOTHING`,
			r.Name, r.Category, r.Price, r.Date,
		)
		if err != nil {
			http.Error(w, "db insert error", http.StatusInternalServerError)
			return
		}

		aff, _ := res.RowsAffected()
		if aff == 0 {
			duplicates++
		} else {
			inserted++
		}
	}

	if err := tx.Commit(); err != nil {
		http.Error(w, "db commit error", http.StatusInternalServerError)
		return
	}

	stats := collectStats()
	stats.TotalCount = totalCount
	stats.DuplicatesCount = duplicates

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(stats)
}

/* ARCHIVES */

func readZip(body []byte, totalCount *int) ([]rowData, error) {
	zr, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		return nil, err
	}

	var rows []rowData
	for _, f := range zr.File {
		if strings.HasSuffix(strings.ToLower(f.Name), ".csv") {
			rc, _ := f.Open()
			part, err := readCSV(rc, totalCount)
			rc.Close()
			if err != nil {
				return nil, err
			}
			rows = append(rows, part...)
		}
	}
	return rows, nil
}

func readTar(body []byte, totalCount *int) ([]rowData, error) {
	tr := tar.NewReader(bytes.NewReader(body))
	var rows []rowData

	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if strings.HasSuffix(strings.ToLower(h.Name), ".csv") {
			part, err := readCSV(tr, totalCount)
			if err != nil {
				return nil, err
			}
			rows = append(rows, part...)
		}
	}
	return rows, nil
}

/* CSV */

func readCSV(r io.Reader, totalCount *int) ([]rowData, error) {
	cr := csv.NewReader(r)
	cr.FieldsPerRecord = -1

	_, err := cr.Read() // header
	if err != nil {
		return nil, err
	}

	var rows []rowData

	for {
		rec, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}

		*totalCount++

		if len(rec) < 5 {
			continue
		}

		name := strings.TrimSpace(rec[1])
		category := strings.TrimSpace(rec[2])
		priceStr := strings.TrimSpace(rec[3])
		dateStr := strings.TrimSpace(rec[4])

		if name == "" || category == "" {
			continue
		}

		price, err := strconv.ParseFloat(priceStr, 64)
		if err != nil {
			continue
		}

		date, err := time.Parse("2006-01-02", dateStr)
		if err != nil {
			continue
		}

		rows = append(rows, rowData{
			Name:     name,
			Category: category,
			Price:    price,
			Date:     date,
		})
	}

	return rows, nil
}

/* GET */

func handleGet(w http.ResponseWriter, r *http.Request) {
	query := `
		SELECT id, name, category, price, create_date
		FROM prices WHERE 1=1
	`
	args := []interface{}{}
	idx := 1

	if v := r.URL.Query().Get("start"); v != "" {
		t, err := time.Parse("2006-01-02", v)
		if err != nil {
			http.Error(w, "invalid start", http.StatusBadRequest)
			return
		}
		query += fmt.Sprintf(" AND create_date >= $%d", idx)
		args = append(args, t)
		idx++
	}

	if v := r.URL.Query().Get("end"); v != "" {
		t, err := time.Parse("2006-01-02", v)
		if err != nil {
			http.Error(w, "invalid end", http.StatusBadRequest)
			return
		}
		query += fmt.Sprintf(" AND create_date <= $%d", idx)
		args = append(args, t)
		idx++
	}

	if v := r.URL.Query().Get("min"); v != "" {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			http.Error(w, "invalid min", http.StatusBadRequest)
			return
		}
		query += fmt.Sprintf(" AND price >= $%d", idx)
		args = append(args, f)
		idx++
	}

	if v := r.URL.Query().Get("max"); v != "" {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			http.Error(w, "invalid max", http.StatusBadRequest)
			return
		}
		query += fmt.Sprintf(" AND price <= $%d", idx)
		args = append(args, f)
		idx++
	}

	query += " ORDER BY id"

	rows, err := db.Query(query, args...)
	if err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	cf, _ := zw.Create("data.csv")
	cw := csv.NewWriter(cf)

	_ = cw.Write([]string{"id", "name", "category", "price", "create_date"})

	for rows.Next() {
		var id int
		var name, category string
		var price float64
		var dt time.Time

		if err := rows.Scan(&id, &name, &category, &price, &dt); err != nil {
			continue
		}

		_ = cw.Write([]string{
			strconv.Itoa(id),
			name,
			category,
			fmt.Sprintf("%.2f", price),
			dt.Format("2006-01-02"),
		})
	}

	cw.Flush()
	_ = zw.Close()

	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", "attachment; filename=data.zip")
	if _, err := w.Write(buf.Bytes()); err != nil {
		log.Printf("write error: %v", err)
	}
}

/* STATS */

func collectStats() Stats {
	var s Stats

	_ = db.QueryRow(`SELECT COUNT(*) FROM prices`).Scan(&s.TotalItems)
	_ = db.QueryRow(`SELECT COUNT(DISTINCT category) FROM prices`).Scan(&s.TotalCategories)
	_ = db.QueryRow(`SELECT COALESCE(SUM(price),0) FROM prices`).Scan(&s.TotalPrice)

	return s
}
