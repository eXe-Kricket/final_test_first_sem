package main

import (
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
	TotalCount      int `json:"total_count"`
	DuplicatesCount int `json:"duplicates_count"`
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
		log.Fatalf("Ошибка открытия БД: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for {
		if err := db.PingContext(ctx); err == nil {
			break
		}
		log.Println("Ожидание PostgreSQL...")
		time.Sleep(2 * time.Second)
	}

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS prices (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			category TEXT NOT NULL,
			price INTEGER NOT NULL,
			UNIQUE(name, category, price)
		)`)
	if err != nil {
		log.Fatalf("Ошибка создания таблицы: %v", err)
	}

	http.HandleFunc("/api/v0/prices", pricesHandler)
	http.HandleFunc("/api/v0/prices/", pricesHandler)

	log.Println("Слушаем на :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func pricesHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		handlePost(w, r)
	case http.MethodGet:
		handleGet(w, r)
	default:
		http.Error(w, "Метод не разрешён", http.StatusMethodNotAllowed)
	}
}

func handlePost(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseMultipartForm(10 << 20); err != nil {
		http.Error(w, "ошибка multipart", http.StatusBadRequest)
		return
	}

	file, _, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "файл отсутствует", http.StatusBadRequest)
		return
	}
	defer file.Close()

	body, err := io.ReadAll(file)
	if err != nil {
		http.Error(w, "ошибка чтения", http.StatusBadRequest)
		return
	}

	zr, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		http.Error(w, "неверный zip", http.StatusBadRequest)
		return
	}

	// Очищаем существующие данные
	_, err = db.Exec("TRUNCATE TABLE prices")
	if err != nil {
		http.Error(w, "ошибка БД", http.StatusInternalServerError)
		return
	}

	totalRowsProcessed := 0
	totalItemsInserted := 0
	duplicatesCount := 0
	totalPrice := 0
	categories := make(map[string]bool)
	seenItems := make(map[string]bool)
	foundCSV := false

	for _, f := range zr.File {
		if !strings.HasSuffix(f.Name, ".csv") {
			continue
		}
		foundCSV = true

		rc, err := f.Open()
		if err != nil {
			continue
		}
		defer rc.Close()

		reader := csv.NewReader(rc)
		reader.Comma = ','
		reader.LazyQuotes = true

		// Читаем все строки
		rows, err := reader.ReadAll()
		if err != nil {
			http.Error(w, "ошибка парсинга CSV", http.StatusBadRequest)
			return
		}

		// Определяем индексы колонок
		var nameIdx, categoryIdx, priceIdx = -1, -1, -1

		if len(rows) > 0 {
			// Пытаемся найти строку заголовка
			for i, cell := range rows[0] {
				cellLower := strings.ToLower(strings.TrimSpace(cell))
				switch cellLower {
				case "name", "product", "item":
					nameIdx = i
				case "category", "type":
					categoryIdx = i
				case "price", "cost":
					priceIdx = i
				}
			}

			// Если заголовок не найден, предполагаем порядок: name, category, price
			if nameIdx == -1 || categoryIdx == -1 || priceIdx == -1 {
				if len(rows[0]) >= 3 {
					nameIdx = 0
					categoryIdx = 1
					priceIdx = 2
				}
			}
		}

		startRow := 0
		if nameIdx >= 0 && categoryIdx >= 0 && priceIdx >= 0 {
			// Проверяем, является ли первая строка заголовком
			firstCellLower := strings.ToLower(strings.TrimSpace(rows[0][0]))
			if strings.Contains(firstCellLower, "name") ||
				strings.Contains(firstCellLower, "category") ||
				strings.Contains(firstCellLower, "price") {
				startRow = 1
			}
		}

		// Обрабатываем строки
		for i := startRow; i < len(rows); i++ {
			row := rows[i]
			totalRowsProcessed++

			if len(row) <= max(nameIdx, categoryIdx, priceIdx) {
				continue
			}

			name := strings.TrimSpace(row[nameIdx])
			category := strings.TrimSpace(row[categoryIdx])
			priceStr := strings.TrimSpace(row[priceIdx])

			if name == "" || category == "" || priceStr == "" {
				continue
			}

			price, err := strconv.Atoi(priceStr)
			if err != nil {
				continue
			}

			// Создаём уникальный ключ для обнаружения дубликатов
			itemKey := fmt.Sprintf("%s|%s|%d", name, category, price)

			if seenItems[itemKey] {
				duplicatesCount++
				continue
			}
			seenItems[itemKey] = true

			// Вставляем в базу данных
			_, err = db.Exec("INSERT INTO prices(name, category, price) VALUES ($1, $2, $3)",
				name, category, price)
			if err != nil {
				// Проверяем, является ли ошибкой дубликата
				if strings.Contains(err.Error(), "duplicate") ||
					strings.Contains(err.Error(), "unique") {
					duplicatesCount++
					continue
				}
				http.Error(w, "ошибка БД", http.StatusInternalServerError)
				return
			}

			totalItemsInserted++
			totalPrice += price
			categories[category] = true
		}
	}

	if !foundCSV {
		http.Error(w, "CSV не найден", http.StatusBadRequest)
		return
	}

	response := Stats{
		TotalCount:      totalRowsProcessed,
		DuplicatesCount: duplicatesCount,
		TotalItems:      totalItemsInserted,
		TotalCategories: len(categories),
		TotalPrice:      totalPrice,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func handleGet(w http.ResponseWriter, r *http.Request) {
	rows, err := db.Query("SELECT name, category, price FROM prices ORDER BY id")
	if err != nil {
		http.Error(w, "ошибка запроса БД", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var buf bytes.Buffer
	zipWriter := zip.NewWriter(&buf)
	csvFile, _ := zipWriter.Create("data.csv")
	csvWriter := csv.NewWriter(csvFile)

	// Записываем заголовок
	csvWriter.Write([]string{"name", "category", "price"})

	for rows.Next() {
		var name, category string
		var price int
		if err := rows.Scan(&name, &category, &price); err != nil {
			continue
		}
		csvWriter.Write([]string{name, category, strconv.Itoa(price)})
	}

	if err = rows.Err(); err != nil {
		http.Error(w, "ошибка БД", http.StatusInternalServerError)
		return
	}

	csvWriter.Flush()
	zipWriter.Close()

	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", "attachment; filename=data.zip")
	w.Write(buf.Bytes())
}

func max(a, b, c int) int {
	max := a
	if b > max {
		max = b
	}
	if c > max {
		max = c
	}
	return max
}
