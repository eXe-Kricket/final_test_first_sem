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

	// Создаем таблицу с дополнительными полями для уровня 3
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS prices (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			category TEXT NOT NULL,
			price INTEGER NOT NULL,
			create_date DATE,
			UNIQUE(name, category, price, create_date)
		)`)
	if err != nil {
		log.Fatalf("Ошибка создания таблицы: %v", err)
	}

	http.HandleFunc("/api/v0/prices", pricesHandler)
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
	queryType := r.URL.Query().Get("type")

	if err := r.ParseMultipartForm(10 << 20); err != nil {
		http.Error(w, "ошибка multipart", http.StatusBadRequest)
		return
	}

	file, fileHeader, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "файл отсутствует", http.StatusBadRequest)
		return
	}
	defer file.Close()

	// Логирование информации о полученном файле
	log.Printf("[POST] Получен файл: %s, размер: %d байт", fileHeader.Filename, fileHeader.Size)

	body, err := io.ReadAll(file)
	if err != nil {
		http.Error(w, "ошибка чтения", http.StatusBadRequest)
		return
	}

	// Очищаем существующие данные перед каждой загрузкой
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

	// Обработка ZIP архива
	if queryType == "zip" || queryType == "" {
		zr, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
		if err != nil {
			http.Error(w, "неверный zip архив", http.StatusBadRequest)
			return
		}

		for _, f := range zr.File {
			if !strings.HasSuffix(f.Name, ".csv") {
				continue
			}

			rc, err := f.Open()
			if err != nil {
				continue
			}
			defer rc.Close()

			err = processCSV(rc, &totalRowsProcessed, &totalItemsInserted, &duplicatesCount,
				&totalPrice, categories, seenItems)
			if err != nil {
				http.Error(w, "ошибка обработки CSV", http.StatusBadRequest)
				return
			}
		}
	} else if queryType == "tar" {
		// Обработка TAR архива (для уровня 2)
		tr := tar.NewReader(bytes.NewReader(body))
		for {
			header, err := tr.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				http.Error(w, "неверный tar архив", http.StatusBadRequest)
				return
			}

			if strings.HasSuffix(header.Name, ".csv") {
				err = processCSV(tr, &totalRowsProcessed, &totalItemsInserted, &duplicatesCount,
					&totalPrice, categories, seenItems)
				if err != nil {
					http.Error(w, "ошибка обработки CSV", http.StatusBadRequest)
					return
				}
			}
		}
	} else {
		http.Error(w, "неподдерживаемый тип архива", http.StatusBadRequest)
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

func processCSV(r io.Reader, totalRowsProcessed, totalItemsInserted, duplicatesCount *int,
	totalPrice *int, categories map[string]bool, seenItems map[string]bool) error {

	reader := csv.NewReader(r)
	reader.Comma = ','
	reader.LazyQuotes = true

	// Читаем первую строку для определения заголовков
	headers, err := reader.Read()
	if err != nil {
		return err
	}

	// Определяем индексы колонок
	nameIdx, categoryIdx, priceIdx, dateIdx := -1, -1, -1, -1
	for i, header := range headers {
		header = strings.ToLower(strings.TrimSpace(header))
		switch {
		case strings.Contains(header, "name") || strings.Contains(header, "product") || strings.Contains(header, "item"):
			nameIdx = i
		case strings.Contains(header, "category") || strings.Contains(header, "type"):
			categoryIdx = i
		case strings.Contains(header, "price") || strings.Contains(header, "cost"):
			priceIdx = i
		case strings.Contains(header, "date") || strings.Contains(header, "create"):
			dateIdx = i
		}
	}

	// Если не нашли заголовки, предполагаем стандартный порядок
	if nameIdx == -1 && len(headers) >= 4 {
		nameIdx = 1
		categoryIdx = 2
		priceIdx = 3
		if len(headers) >= 5 {
			dateIdx = 4
		}
	}

	// Обрабатываем строки
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue // Пропускаем некорректные строки
		}

		*totalRowsProcessed++

		if nameIdx >= len(row) || categoryIdx >= len(row) || priceIdx >= len(row) {
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

		// Обрабатываем дату (опционально)
		var createDate *time.Time
		if dateIdx != -1 && dateIdx < len(row) && row[dateIdx] != "" {
			dateStr := strings.TrimSpace(row[dateIdx])
			if parsedDate, err := time.Parse("2006-01-02", dateStr); err == nil {
				createDate = &parsedDate
			}
		}

		// Создаем уникальный ключ
		dateKey := ""
		if createDate != nil {
			dateKey = createDate.Format("2006-01-02")
		}
		itemKey := fmt.Sprintf("%s|%s|%d|%s", name, category, price, dateKey)

		if seenItems[itemKey] {
			*duplicatesCount++
			continue
		}
		seenItems[itemKey] = true

		// Вставляем в базу данных
		var errInsert error
		if createDate != nil {
			_, errInsert = db.Exec(
				"INSERT INTO prices(name, category, price, create_date) VALUES ($1, $2, $3, $4)",
				name, category, price, createDate)
		} else {
			_, errInsert = db.Exec(
				"INSERT INTO prices(name, category, price) VALUES ($1, $2, $3)",
				name, category, price)
		}

		if errInsert != nil {
			if strings.Contains(errInsert.Error(), "duplicate") ||
				strings.Contains(errInsert.Error(), "unique") {
				*duplicatesCount++
				continue
			}
			return errInsert
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

	rows, err := db.Query(query, args...)
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
	csvWriter.Write([]string{"name", "category", "price", "create_date"})

	for rows.Next() {
		var name, category string
		var price int
		var createDate sql.NullTime

		if err := rows.Scan(&name, &category, &price, &createDate); err != nil {
			continue
		}

		dateStr := ""
		if createDate.Valid {
			dateStr = createDate.Time.Format("2006-01-02")
		}

		csvWriter.Write([]string{name, category, strconv.Itoa(price), dateStr})
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
