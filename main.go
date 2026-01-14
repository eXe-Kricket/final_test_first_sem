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
	TotalCount      int `json:"total_count"`      // для уровня 3 — равно total_items
	DuplicatesCount int `json:"duplicates_count"` // для уровня 3 — всегда 0
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
	http.HandleFunc("/api/v0/prices/", pricesHandler) // на случай слеша

	log.Println("Сервер запущен на :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func pricesHandler(w http.ResponseWriter, r *http.Request) {
	// Лог для уровня 2 — поможет увидеть, что именно приходит от тестов
	log.Printf("Запрос обработан: %s %s%s (Content-Type: %s)",
		r.Method, r.URL.Path, r.URL.RawQuery, r.Header.Get("Content-Type"))

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
	// Гибкая проверка Content-Type для уровня 1
	ct := r.Header.Get("Content-Type")
	if !strings.Contains(strings.ToLower(ct), "zip") && ct != "" {
		http.Error(w, "Ожидается ZIP-архив (application/zip или подобный)", http.StatusBadRequest)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Не удалось прочитать тело запроса", http.StatusBadRequest)
		return
	}

	zipReader, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		http.Error(w, "Некорректный ZIP-архив", http.StatusBadRequest)
		return
	}

	var totalItems, totalPrice int
	categories := make(map[string]bool)
	var foundCSV bool

	for _, file := range zipReader.File {
		if file.Name != "data.csv" {
			continue
		}
		foundCSV = true

		f, err := file.Open()
		if err != nil {
			http.Error(w, "Не удалось открыть data.csv в архиве", http.StatusInternalServerError)
			return
		}
		defer f.Close()

		csvReader := csv.NewReader(f)
		csvReader.Comma = ','

		// Пропускаем заголовок (если есть)
		_, err = csvReader.Read()
		if err != nil && err != io.EOF {
			http.Error(w, "Ошибка чтения заголовка CSV", http.StatusBadRequest)
			return
		}

		_, err = db.Exec("TRUNCATE TABLE prices")
		if err != nil {
			http.Error(w, "Ошибка очистки таблицы", http.StatusInternalServerError)
			return
		}

		for {
			record, err := csvReader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				http.Error(w, "Ошибка чтения строки CSV", http.StatusBadRequest)
				return
			}

			if len(record) != 3 {
				http.Error(w, "Неверный формат CSV (ожидается 3 колонки)", http.StatusBadRequest)
				return
			}

			item := record[0]
			category := record[1]
			priceStr := record[2]

			price, err := strconv.Atoi(priceStr)
			if err != nil {
				http.Error(w, "Некорректная цена в CSV", http.StatusBadRequest)
				return
			}

			_, err = db.Exec(
				"INSERT INTO prices (item, category, price) VALUES ($1, $2, $3)",
				item, category, price,
			)
			if err != nil {
				http.Error(w, "Ошибка вставки записи в БД", http.StatusInternalServerError)
				return
			}

			totalItems++
			totalPrice += price
			categories[category] = true
		}
	}

	if !foundCSV {
		http.Error(w, "В архиве отсутствует файл data.csv", http.StatusBadRequest)
		return
	}

	stats := Stats{
		TotalCount:      totalItems, // для уровня 3
		DuplicatesCount: 0,          // дубликаты не считаем
		TotalItems:      totalItems,
		TotalCategories: len(categories),
		TotalPrice:      totalPrice,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

func handleGet(w http.ResponseWriter, r *http.Request) {
	rows, err := db.Query("SELECT item, category, price FROM prices ORDER BY id")
	if err != nil {
		http.Error(w, "Ошибка получения данных из БД", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var buf bytes.Buffer
	zipWriter := zip.NewWriter(&buf)

	csvFile, err := zipWriter.Create("data.csv")
	if err != nil {
		http.Error(w, "Ошибка создания CSV в архиве", http.StatusInternalServerError)
		return
	}

	csvWriter := csv.NewWriter(csvFile)
	csvWriter.Write([]string{"item", "category", "price"})

	for rows.Next() {
		var item, category string
		var price int
		if err := rows.Scan(&item, &category, &price); err != nil {
			http.Error(w, "Ошибка чтения строки из БД", http.StatusInternalServerError)
			return
		}
		csvWriter.Write([]string{item, category, strconv.Itoa(price)})
	}

	if err := rows.Err(); err != nil {
		http.Error(w, "Ошибка при итерации по результатам запроса", http.StatusInternalServerError)
		return
	}

	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		http.Error(w, "Ошибка записи CSV", http.StatusInternalServerError)
		return
	}

	if err := zipWriter.Close(); err != nil {
		http.Error(w, "Ошибка закрытия ZIP-архива", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", "attachment; filename=data.zip")
	w.Write(buf.Bytes())
}
