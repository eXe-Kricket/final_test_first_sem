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
	log.Printf("[POST] Content-Type: %q | Content-Length: %d", r.Header.Get("Content-Type"), r.ContentLength)

	// Парсим multipart/form-data (основной формат в тестах)
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		log.Printf("[ERROR] ParseMultipartForm failed: %v", err)
		http.Error(w, "Не удалось разобрать multipart/form-data", http.StatusBadRequest)
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		log.Printf("[ERROR] Поле 'file' не найдено: %v", err)
		http.Error(w, "Отсутствует поле 'file' в запросе", http.StatusBadRequest)
		return
	}
	defer file.Close()

	log.Printf("[POST] Получен файл: %q, размер: %d байт", header.Filename, header.Size)

	body, err := io.ReadAll(file)
	if err != nil {
		log.Printf("[ERROR] Ошибка чтения файла: %v", err)
		http.Error(w, "Не удалось прочитать файл", http.StatusInternalServerError)
		return
	}

	log.Printf("[POST] Размер тела файла: %d байт", len(body))

	if len(body) == 0 {
		log.Println("[ERROR] Получен пустой файл")
		http.Error(w, "Пустой файл", http.StatusBadRequest)
		return
	}

	zipReader, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		log.Printf("[ERROR] Не удалось распарсить ZIP: %v", err)
		http.Error(w, "Невозможно распарсить как ZIP", http.StatusBadRequest)
		return
	}

	log.Printf("[ZIP] Файлов в архиве: %d", len(zipReader.File))

	var totalItems, totalPrice int
	categories := make(map[string]bool)
	var foundCSV bool

	for _, file := range zipReader.File {
		log.Printf("[ZIP] Файл: %s (размер %d байт)", file.Name, file.UncompressedSize64)

		if file.Name != "data.csv" && !strings.HasSuffix(strings.ToLower(file.Name), ".csv") {
			log.Printf("[ZIP] Пропущен (не .csv): %s", file.Name)
			continue
		}

		foundCSV = true

		f, err := file.Open()
		if err != nil {
			log.Printf("[ERROR] Не удалось открыть %s: %v", file.Name, err)
			http.Error(w, "Не удалось открыть CSV", http.StatusInternalServerError)
			return
		}
		defer f.Close()

		csvReader := csv.NewReader(f)
		csvReader.Comma = ','
		csvReader.LazyQuotes = true

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
			log.Println("[CSV] Нет заголовка — первая строка считается данными")
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
				log.Printf("[ERROR] Ошибка чтения строки: %v", err)
				http.Error(w, "Ошибка чтения CSV", http.StatusBadRequest)
				return
			}

			rowCount++
			if rowCount <= 5 {
				log.Printf("[CSV] Строка %d: %v", rowCount, record)
			}

			if len(record) != 3 {
				log.Printf("[ERROR] Строка %d: неверное кол-во колонок (%d)", rowCount, len(record))
				http.Error(w, "Неверный формат CSV (3 колонки)", http.StatusBadRequest)
				return
			}

			item := record[0]
			category := record[1]
			priceStr := record[2]

			price, err := strconv.Atoi(priceStr)
			if err != nil {
				log.Printf("[ERROR] Строка %d: некорректная цена %q → %v", rowCount, priceStr, err)
				http.Error(w, "Некорректная цена", http.StatusBadRequest)
				return
			}

			_, err = db.Exec("INSERT INTO prices (item, category, price) VALUES ($1, $2, $3)",
				item, category, price)
			if err != nil {
				log.Printf("[ERROR] Ошибка INSERT строка %d: %v", rowCount, err)
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
		log.Println("[ERROR] CSV-файл не найден в архиве")
		http.Error(w, "CSV-файл не найден", http.StatusBadRequest)
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
	if err := json.NewEncoder(w).Encode(stats); err != nil {
		log.Printf("[ERROR] Ошибка кодирования JSON: %v", err)
	}

	log.Printf("[POST SUCCESS] Добавлено: %d строк, категорий: %d, сумма: %d",
		totalItems, len(categories), totalPrice)
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
		http.Error(w, "Ошибка создания CSV", http.StatusInternalServerError)
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
		http.Error(w, "Ошибка итерации результатов", http.StatusInternalServerError)
		return
	}

	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		http.Error(w, "Ошибка записи CSV", http.StatusInternalServerError)
		return
	}

	if err := zipWriter.Close(); err != nil {
		http.Error(w, "Ошибка закрытия ZIP", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", "attachment; filename=data.zip")
	w.Write(buf.Bytes())
}
