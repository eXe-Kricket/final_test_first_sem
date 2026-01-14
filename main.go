package main

import (
	"archive/zip"
	"bytes"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strconv"

	_ "github.com/lib/pq"
)

type Stats struct {
	TotalItems      int `json:"total_items"`
	TotalCategories int `json:"total_categories"`
	TotalPrice      int `json:"total_price"`
}

var db *sql.DB

func main() {
	var err error
	connStr := "user=validator password=val1dat0r dbname=project-sem-1 sslmode=disable"
	db, err = sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Создаем таблицу, если не существует
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS prices (
		id SERIAL PRIMARY KEY,
		item TEXT,
		category TEXT,
		price INTEGER
	)`)
	if err != nil {
		log.Fatal(err)
	}

	http.HandleFunc("/api/v0/prices", pricesHandler)
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func pricesHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "POST":
		handlePost(w, r)
	case "GET":
		handleGet(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func handlePost(w http.ResponseWriter, r *http.Request) {
	// Читаем тело запроса как zip
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Распаковываем zip
	zipReader, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var totalItems, totalPrice int
	categories := make(map[string]bool)

	for _, file := range zipReader.File {
		if file.Name != "data.csv" {
			continue
		}

		f, err := file.Open()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer f.Close()

		csvReader := csv.NewReader(f)
		csvReader.Comma = ','

		// Пропускаем заголовок, предполагаем формат: item,category,price
		_, err = csvReader.Read()
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// Очищаем таблицу перед вставкой
		_, err = db.Exec("TRUNCATE TABLE prices")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		for {
			record, err := csvReader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			if len(record) != 3 {
				http.Error(w, "Invalid CSV format", http.StatusBadRequest)
				return
			}

			item := record[0]
			category := record[1]
			priceStr := record[2]
			price, err := strconv.Atoi(priceStr)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			_, err = db.Exec("INSERT INTO prices (item, category, price) VALUES ($1, $2, $3)", item, category, price)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			totalItems++
			totalPrice += price
			categories[category] = true
		}
	}

	stats := Stats{
		TotalItems:      totalItems,
		TotalCategories: len(categories),
		TotalPrice:      totalPrice,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

func handleGet(w http.ResponseWriter, r *http.Request) {
	// Выгружаем данные из БД
	rows, err := db.Query("SELECT item, category, price FROM prices")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var buf bytes.Buffer
	zipWriter := zip.NewWriter(&buf)

	csvFile, err := zipWriter.Create("data.csv")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	csvWriter := csv.NewWriter(csvFile)
	csvWriter.Write([]string{"item", "category", "price"})

	for rows.Next() {
		var item, category string
		var price int
		err := rows.Scan(&item, &category, &price)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		csvWriter.Write([]string{item, category, strconv.Itoa(price)})
	}

	// Проверяем ошибки после цикла
	if err = rows.Err(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	zipWriter.Close()

	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", "attachment; filename=data.zip")
	w.Write(buf.Bytes())
}
