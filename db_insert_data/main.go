package main

import (
	"database/sql"          // Provides SQL database functionality
	"fmt"                   // For printing messages
	"os"
	"github.com/joho/godotenv"
	"log"                   // For logging errors
	_ "github.com/lib/pq"   // Import pq for PostgreSQL driver
	"encoding/csv"
	"strconv"
)

type env_vars struct {
	db string
	host string
	schema string
	user string
	pass string
}

type MovieRecord struct {
    title string
	movie_type string
	genres string
	average_rate float64
	num_votes int8
	release_year int8
}

var csv_data = ""

func main() {
	// Define the connection string with PostgreSQL credentials
	godotenv.Load()
	var env_dict = setUpEnvVars()
	connStr := fmt.Sprintf("user=%v password=%v dbname=%v sslmode=disable", env_dict.user, env_dict.pass, env_dict.db)
	db, err := sql.Open("postgres", connStr)
	
	// Open a database connection
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close() // Ensure connection closes after function ends

	// Ping to confirm connection
	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Connected to PostgreSQL successfully!")
	createTable(db)
	
	var read_data = readCSVData(csv_data)
	// convert records to array of structs
	movieList := createMoviesList(read_data)
	
	for i := 0; i < len(movieList); i++ {
		rec := movieList[i]
		insertVals(db, rec.title, rec.movie_type, rec.genres, rec.average_rate, rec.num_votes, rec.release_year)

	}
	fmt.Println("Successfully inserted all values")
	
}

func readCSVData(csv_path string) [][]string {
	// open file
	f, err := os.Open(csv_path)
	if err != nil {
		log.Fatal(err)
	}

	// remember to close the file at the end of the program
	defer f.Close()

	// read csv values using csv.Reader
	csvReader := csv.NewReader(f)
	data, err := csvReader.ReadAll()
	if err != nil {
		log.Fatal(err)
	}

	return data
}

func createTable(db *sql.DB) {
	// Define the SQL query for creating a new table
	createTableSQL := `
	CREATE TABLE IF NOT EXISTS movies (
		id SERIAL PRIMARY KEY,
		title VARCHAR(255),
		type VARCHAR(100),
		genres VARCHAR(100),
		average_rating NUMERIC(10,2),
		num_votes INTEGER,
		release_year INTEGER
	);`
	
	// Execute the SQL query
	_, err := db.Exec(createTableSQL)
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}
	fmt.Println("Table created successfully.")
}

func createMoviesList(data [][]string) []MovieRecord {
    var movieList []MovieRecord
    for i, line := range data {
        if i > 0 { // skip header line
            var rec MovieRecord
            for j, field := range line {
				
				if j == 1 {
					rec.title = field
				} else if j == 2 {
					rec.movie_type = field
				} else if j == 3 {
					rec.genres = field
				} else if j == 4 {
					if field == ""{
						rec.average_rate = 0.0
					} else{
						result := stringToFloat(field)
						rec.average_rate = float64(result)
					}
					
				} else if j == 5 {
					if field == ""{
						rec.num_votes = 0
					} else{
						result := stringToInt(field)
						rec.num_votes = int8(result)
					}
				} else if j == 6 {
					if field == ""{
						rec.release_year = 0
					} else{
						result := stringToInt(field)
						rec.release_year = int8(result)
					}
				}
            }
            movieList = append(movieList, rec)
        }
    }
    return movieList
}

func stringToFloat(input string) float64{
	float64Value, err := strconv.ParseFloat(input, 64)
    if err != nil {
        fmt.Println("Error parsing float:", err)
    }
	return float64Value
}

func stringToInt(input string) int{
	intValue, err := strconv.Atoi(input)
	if err != nil {
		fmt.Println("Error converting string to int:", err)
	}
	return intValue
}

func insertVals(db *sql.DB, mov_title string, mov_type string, mov_genres string, avg_rate float64, num_vote int8, mov_year int8) bool{
	insert_vals := fmt.Sprintf(
		`INSERT INTO movies(
			title, type, genres, average_rating, num_votes, release_year
		) 
		VALUES(
			$$%v$$, '%v', '%v', %v, %v, %v
		);`,mov_title, mov_type, mov_genres, avg_rate, num_vote, mov_year)

	// Execute the SQL query
	_, err := db.Exec(insert_vals)
	if err != nil {
		log.Fatalf("Values to insert: %v, %v, %v, %v, %v, %v",mov_title, mov_type, mov_genres, avg_rate, num_vote, mov_year)
		log.Fatalf("Failed to insert values: %v", err)
		return false
	} else{
		return true
	}

}

func setUpEnvVars() env_vars {
	SHELL := os.Getenv("DB_N4ME")
	GOPATH := os.Getenv("HOST")
	GOSCHEM := os.Getenv("SCH3MA_N4ME")
	GOUSR := os.Getenv("US3ER")
	GOPASS := os.Getenv("PASWRO")

	return env_vars {
		db: SHELL, 
		host: GOPATH, 
		schema: GOSCHEM, 
		user: GOUSR, 
		pass: GOPASS,
	}
}