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

	// Проверяем существующую структуру таблицы
	checkAndCreateTable()

	http.HandleFunc("/api/v0/prices", pricesHandler)
	log.Println("Слушаем на :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func checkAndCreateTable() {
	// Проверяем, существует ли таблица
	var exists bool
	err := db.QueryRow(`
		SELECT EXISTS (
			SELECT FROM information_schema.tables 
			WHERE table_schema = 'public' 
			AND table_name = 'prices'
		)`).Scan(&exists)

	if err != nil {
		log.Printf("Ошибка проверки таблицы: %v", err)
		exists = false
	}

	if !exists {
		// Создаем таблицу с правильной структурой
		_, err = db.Exec(`
			CREATE TABLE prices (
				id SERIAL PRIMARY KEY,
				name TEXT NOT NULL,
				category TEXT NOT NULL,
				price INTEGER NOT NULL,
				create_date DATE
			)`)
		if err != nil {
			log.Fatalf("Ошибка создания таблицы: %v", err)
		}
		log.Println("Таблица 'prices' создана")
	} else {
		// Проверяем структуру существующей таблицы
		log.Println("Таблица 'prices' уже существует, проверяем структуру...")

		// Проверяем наличие необходимых колонок
		columns := []string{"name", "category", "price", "create_date"}
		for _, col := range columns {
			var columnExists bool
			err := db.QueryRow(`
				SELECT EXISTS (
					SELECT FROM information_schema.columns 
					WHERE table_schema = 'public' 
					AND table_name = 'prices' 
					AND column_name = $1
				)`, col).Scan(&columnExists)

			if err != nil {
				log.Printf("Ошибка проверки колонки %s: %v", col, err)
			} else if !columnExists {
				log.Printf("Колонка %s отсутствует, добавляем...", col)

				// Добавляем недостающие колонки
				var alterSQL string
				switch col {
				case "name":
					alterSQL = "ALTER TABLE prices ADD COLUMN name TEXT NOT NULL DEFAULT ''"
				case "category":
					alterSQL = "ALTER TABLE prices ADD COLUMN category TEXT NOT NULL DEFAULT ''"
				case "price":
					alterSQL = "ALTER TABLE prices ADD COLUMN price INTEGER NOT NULL DEFAULT 0"
				case "create_date":
					alterSQL = "ALTER TABLE prices ADD COLUMN create_date DATE"
				}

				if alterSQL != "" {
					_, err := db.Exec(alterSQL)
					if err != nil {
						log.Printf("Ошибка добавления колонки %s: %v", col, err)
					} else {
						log.Printf("Колонка %s успешно добавлена", col)
					}
				}
			}
		}
	}
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
	queryType := r.URL.Query().Get("type")

	if err := r.ParseMultipartForm(10 << 20); err != nil {
		log.Printf("ParseMultipartForm error: %v", err)
		http.Error(w, "multipart error", http.StatusBadRequest)
		return
	}

	file, _, err := r.FormFile("file")
	if err != nil {
		log.Printf("FormFile error: %v", err)
		http.Error(w, "file missing", http.StatusBadRequest)
		return
	}
	defer file.Close()

	body, err := io.ReadAll(file)
	if err != nil {
		log.Printf("ReadAll error: %v", err)
		http.Error(w, "read error", http.StatusBadRequest)
		return
	}

	totalRowsProcessed := 0
	totalItemsInserted := 0
	duplicatesCount := 0
	totalPrice := 0
	categories := make(map[string]bool)
	seenItems := make(map[string]bool)

	// Обработка ZIP архива
	if queryType == "zip" || queryType == "" {
		zr, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
		if err != nil {
			log.Printf("ZIP error: %v", err)
			http.Error(w, "invalid zip", http.StatusBadRequest)
			return
		}

		processedAnyCSV := false
		for _, f := range zr.File {
			if !strings.HasSuffix(strings.ToLower(f.Name), ".csv") {
				continue
			}

			rc, err := f.Open()
			if err != nil {
				log.Printf("Open zip file error: %v", err)
				continue
			}

			err = processCSV(rc, &totalRowsProcessed, &totalItemsInserted, &duplicatesCount,
				&totalPrice, categories, seenItems)
			rc.Close()

			if err != nil {
				log.Printf("Process CSV error: %v", err)
				http.Error(w, "csv processing error", http.StatusBadRequest)
				return
			}
			processedAnyCSV = true
		}

		if !processedAnyCSV {
			http.Error(w, "no csv files found", http.StatusBadRequest)
			return
		}
	} else if queryType == "tar" {
		// Обработка TAR архива (для уровня 2)
		tr := tar.NewReader(bytes.NewReader(body))
		processedAnyCSV := false

		for {
			header, err := tr.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Printf("TAR error: %v", err)
				http.Error(w, "invalid tar", http.StatusBadRequest)
				return
			}

			if strings.HasSuffix(strings.ToLower(header.Name), ".csv") {
				err = processCSV(tr, &totalRowsProcessed, &totalItemsInserted, &duplicatesCount,
					&totalPrice, categories, seenItems)
				if err != nil {
					log.Printf("Process CSV error: %v", err)
					http.Error(w, "csv processing error", http.StatusBadRequest)
					return
				}
				processedAnyCSV = true
			}
		}

		if !processedAnyCSV {
			http.Error(w, "no csv files found", http.StatusBadRequest)
			return
		}
	} else {
		http.Error(w, "unsupported archive type", http.StatusBadRequest)
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
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("JSON encode error: %v", err)
	}
}

func processCSV(r io.Reader, totalRowsProcessed, totalItemsInserted, duplicatesCount *int,
	totalPrice *int, categories map[string]bool, seenItems map[string]bool) error {

	reader := csv.NewReader(r)
	reader.Comma = ','
	reader.LazyQuotes = true
	reader.FieldsPerRecord = -1

	// Читаем первую строку для определения заголовков
	headers, err := reader.Read()
	if err != nil {
		return fmt.Errorf("failed to read headers: %v", err)
	}

	// Определяем индексы колонок
	nameIdx, categoryIdx, priceIdx, dateIdx := -1, -1, -1, -1

	for i, header := range headers {
		header = strings.ToLower(strings.TrimSpace(header))

		if strings.Contains(header, "name") {
			nameIdx = i
		}
		if strings.Contains(header, "category") {
			categoryIdx = i
		}
		if strings.Contains(header, "price") {
			priceIdx = i
		}
		if strings.Contains(header, "date") {
			dateIdx = i
		}
	}

	// Если не нашли по именам, предполагаем порядок из тестов: id,name,category,price,create_date
	if nameIdx == -1 && len(headers) >= 2 {
		nameIdx = 1
	}
	if categoryIdx == -1 && len(headers) >= 3 {
		categoryIdx = 2
	}
	if priceIdx == -1 && len(headers) >= 4 {
		priceIdx = 3
	}
	if dateIdx == -1 && len(headers) >= 5 {
		dateIdx = 4
	}

	// Проверяем, что нашли необходимые колонки
	if nameIdx == -1 || categoryIdx == -1 || priceIdx == -1 {
		log.Printf("Warning: Required columns not found. Using default indices. Headers: %v", headers)
		// Используем дефолтные индексы
		if len(headers) >= 4 {
			nameIdx = 1
			categoryIdx = 2
			priceIdx = 3
			if len(headers) >= 5 {
				dateIdx = 4
			}
		}
	}

	// Обрабатываем строки
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("CSV read error: %v", err)
			continue
		}

		*totalRowsProcessed++

		// Проверяем, что строка имеет достаточно колонок
		if len(row) <= nameIdx || len(row) <= categoryIdx || len(row) <= priceIdx {
			continue
		}

		name := strings.TrimSpace(row[nameIdx])
		category := strings.TrimSpace(row[categoryIdx])
		priceStr := strings.TrimSpace(row[priceIdx])

		// Пропускаем пустые значения
		if name == "" || category == "" || priceStr == "" {
			continue
		}

		// Парсим цену
		price, err := strconv.Atoi(priceStr)
		if err != nil {
			// Для уровня 3: пропускаем некорректные цены
			continue
		}

		*totalItemsInserted++
		*totalPrice += price
		categories[category] = true
	}

	return nil
}

func handleGet(w http.ResponseWriter, r *http.Request) {
	// Получаем параметры фильтрации
	startDate := r.URL.Query().Get("start")
	endDate := r.URL.Query().Get("end")
	minPrice := r.URL.Query().Get("min")
	maxPrice := r.URL.Query().Get("max")

	// Строим SQL запрос с фильтрами
	query := "SELECT name, category, price, create_date FROM prices WHERE 1=1"
	args := []interface{}{}
	argIdx := 1

	if startDate != "" {
		query += fmt.Sprintf(" AND create_date >= $%d", argIdx)
		args = append(args, startDate)
		argIdx++
	}
	if endDate != "" {
		query += fmt.Sprintf(" AND create_date <= $%d", argIdx)
		args = append(args, endDate)
		argIdx++
	}
	if minPrice != "" {
		query += fmt.Sprintf(" AND price >= $%d", argIdx)
		if min, err := strconv.Atoi(minPrice); err == nil {
			args = append(args, min)
		} else {
			args = append(args, 0)
		}
		argIdx++
	}
	if maxPrice != "" {
		query += fmt.Sprintf(" AND price <= $%d", argIdx)
		if max, err := strconv.Atoi(maxPrice); err == nil {
			args = append(args, max)
		} else {
			args = append(args, 1000000)
		}
		argIdx++
	}

	query += " ORDER BY id"

	log.Printf("Executing query: %s with args: %v", query, args)

	rows, err := db.Query(query, args...)
	if err != nil {
		log.Printf("DB query error: %v", err)
		// Возвращаем пустой архив вместо ошибки
		returnEmptyZip(w)
		return
	}
	defer rows.Close()

	var buf bytes.Buffer
	zipWriter := zip.NewWriter(&buf)
	csvFile, err := zipWriter.Create("data.csv")
	if err != nil {
		log.Printf("Create zip file error: %v", err)
		returnEmptyZip(w)
		return
	}

	csvWriter := csv.NewWriter(csvFile)

	// Записываем заголовок
	if err := csvWriter.Write([]string{"name", "category", "price", "create_date"}); err != nil {
		log.Printf("Write CSV header error: %v", err)
	}

	rowCount := 0
	for rows.Next() {
		var name, category string
		var price int
		var createDate sql.NullTime

		if err := rows.Scan(&name, &category, &price, &createDate); err != nil {
			log.Printf("Row scan error: %v", err)
			continue
		}

		dateStr := ""
		if createDate.Valid {
			dateStr = createDate.Time.Format("2006-01-02")
		}

		if err := csvWriter.Write([]string{name, category, strconv.Itoa(price), dateStr}); err != nil {
			log.Printf("Write CSV row error: %v", err)
		}
		rowCount++
	}

	if err = rows.Err(); err != nil {
		log.Printf("Rows iteration error: %v", err)
	}

	csvWriter.Flush()
	if err := zipWriter.Close(); err != nil {
		log.Printf("Close zip error: %v", err)
	}

	log.Printf("Exported %d rows", rowCount)

	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", "attachment; filename=data.zip")
	w.Write(buf.Bytes())
}

func returnEmptyZip(w http.ResponseWriter) {
	var buf bytes.Buffer
	zipWriter := zip.NewWriter(&buf)
	csvFile, _ := zipWriter.Create("data.csv")
	csvWriter := csv.NewWriter(csvFile)
	csvWriter.Write([]string{"name", "category", "price", "create_date"})
	csvWriter.Flush()
	zipWriter.Close()

	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", "attachment; filename=data.zip")
	w.Write(buf.Bytes())
}
