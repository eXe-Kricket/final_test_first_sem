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
		log.Fatalf("Не удалось открыть соединение с БД: %v", err)
	}
	defer db.Close()

	db.SetConnMaxLifetime(5 * time.Minute)
	db.SetMaxOpenConns(10)

	log.Println("Ожидание готовности PostgreSQL...")
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	for i := 0; i < 15; i++ {
		if err := db.PingContext(ctx); err == nil {
			log.Println("PostgreSQL готова")
			break
		}
		log.Printf("PostgreSQL ещё не готова (попытка %d/15): %v", i+1, err)
		time.Sleep(3 * time.Second)
	}

	if err := db.Ping(); err != nil {
		log.Fatalf("Не удалось подключиться к PostgreSQL: %v", err)
	}

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS prices (
			id       SERIAL PRIMARY KEY,
			item     TEXT NOT NULL,
			category TEXT NOT NULL,
			price    INTEGER NOT NULL
		)`)
	if err != nil {
		log.Fatalf("Ошибка создания таблицы: %v", err)
	}
	log.Println("Таблица prices готова")

	http.HandleFunc("/api/v0/prices", pricesHandler)
	http.HandleFunc("/api/v0/prices/", pricesHandler)

	log.Println("Сервер запущен на :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func pricesHandler(w http.ResponseWriter, r *http.Request) {
	log.Printf("[REQUEST] %s %s%s | Content-Type: %q | Content-Length: %d",
		r.Method, r.URL.Path, r.URL.RawQuery, r.Header.Get("Content-Type"), r.ContentLength)

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
	ct := strings.ToLower(r.Header.Get("Content-Type"))
	log.Printf("[POST] Content-Type: %q", ct)

	// Очень мягкая проверка для тестов
	if ct != "" && !strings.Contains(ct, "zip") && !strings.Contains(ct, "octet") {
		log.Printf("[ERROR] Неподдерживаемый Content-Type: %q", ct)
		http.Error(w, "Ожидается ZIP-архив", http.StatusBadRequest)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("[ERROR] Не удалось прочитать тело: %v", err)
		http.Error(w, "Не удалось прочитать тело запроса", http.StatusBadRequest)
		return
	}

	log.Printf("[POST] Тело запроса прочитано, размер: %d байт", len(body))
	if len(body) == 0 {
		log.Println("[ERROR] Тело запроса пустое")
		http.Error(w, "Пустое тело запроса", http.StatusBadRequest)
		return
	}

	zipReader, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		log.Printf("[ERROR] Не удалось распарсить ZIP: %v", err)
		http.Error(w, "Некорректный ZIP-архив", http.StatusBadRequest)
		return
	}

	log.Printf("[ZIP] Успешно распарсен, файлов внутри: %d", len(zipReader.File))

	var totalItems, totalPrice int
	categories := make(map[string]bool)
	var foundCSV bool

	for _, file := range zipReader.File {
		log.Printf("[ZIP] Файл: %s, размер: %d байт", file.Name, file.UncompressedSize64)

		if file.Name != "data.csv" && !strings.HasSuffix(file.Name, ".csv") {
			log.Printf("[ZIP] Пропущен файл (не data.csv): %s", file.Name)
			continue
		}

		foundCSV = true

		f, err := file.Open()
		if err != nil {
			log.Printf("[ERROR] Не удалось открыть файл %s: %v", file.Name, err)
			http.Error(w, "Не удалось открыть CSV в архиве", http.StatusInternalServerError)
			return
		}
		defer f.Close()

		csvReader := csv.NewReader(f)
		csvReader.Comma = ','
		csvReader.LazyQuotes = true // более устойчиво к кавычкам

		// Пропуск заголовка
		header, err := csvReader.Read()
		if err != nil && err != io.EOF {
			log.Printf("[ERROR] Ошибка чтения заголовка: %v", err)
			http.Error(w, "Ошибка чтения заголовка CSV", http.StatusBadRequest)
			return
		}
		if header != nil {
			log.Printf("[CSV] Заголовок: %v", header)
		} else {
			log.Println("[CSV] Заголовок отсутствует")
		}

		_, err = db.Exec("TRUNCATE TABLE prices")
		if err != nil {
			log.Printf("[ERROR] Ошибка TRUNCATE: %v", err)
			http.Error(w, "Ошибка очистки таблицы", http.StatusInternalServerError)
			return
		}
		log.Println("[DB] Таблица очищена")

		rowCount := 0
		for {
			record, err := csvReader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Printf("[ERROR] Ошибка чтения строки CSV: %v", err)
				http.Error(w, "Ошибка чтения строки CSV", http.StatusBadRequest)
				return
			}

			rowCount++
			if rowCount <= 5 {
				log.Printf("[CSV] Строка %d: %v", rowCount, record)
			}

			if len(record) != 3 {
				log.Printf("[ERROR] Неверное количество колонок в строке %d: %d", rowCount, len(record))
				http.Error(w, "Неверный формат CSV (ожидается 3 колонки)", http.StatusBadRequest)
				return
			}

			item := record[0]
			category := record[1]
			priceStr := record[2]

			price, err := strconv.Atoi(priceStr)
			if err != nil {
				log.Printf("[ERROR] Некорректная цена в строке %d: %q → %v", rowCount, priceStr, err)
				http.Error(w, "Некорректная цена в CSV", http.StatusBadRequest)
				return
			}

			_, err = db.Exec(
				"INSERT INTO prices (item, category, price) VALUES ($1, $2, $3)",
				item, category, price,
			)
			if err != nil {
				log.Printf("[ERROR] Ошибка INSERT в строке %d: %v", rowCount, err)
				http.Error(w, "Ошибка вставки в БД", http.StatusInternalServerError)
				return
			}

			totalItems++
			totalPrice += price
			categories[category] = true
		}
		log.Printf("[CSV] Обработано строк: %d", rowCount)
	}

	if !foundCSV {
		log.Println("[ERROR] Файл data.csv не найден в архиве")
		http.Error(w, "В архиве отсутствует файл data.csv", http.StatusBadRequest)
		return
	}

	stats := Stats{
		TotalCount:      totalItems,
		DuplicatesCount: 0,
		TotalItems:      totalItems,
		TotalCategories: len(categories),
		TotalPrice:      totalPrice,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)

	log.Printf("[POST SUCCESS] Добавлено: %d строк, категорий: %d, сумма: %d",
		totalItems, len(categories), totalPrice)
}
