package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type recordWithIndex struct {
	record []string
	index  int
}

func main() {
	start := time.Now()
	err := SplitCsvFile("ratings.csv", "movies.csv", 10)
	if err != nil {
		fmt.Println("Error:", err)
	}
	duration := time.Since(start)
	fmt.Printf("Tiempo de ejecución: %v\n", duration)
}

// SplitCsvFile divide el archivo CSV en partes más pequeñas y cuenta los ratings por género
func SplitCsvFile(ratingsFile, moviesFile string, numFiles int) error {
	recordCh := make(chan recordWithIndex, 100)
	var wg sync.WaitGroup

	// Lanzamos goroutines para escribir en los archivos
	for i := 0; i < numFiles; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			outputFile := fmt.Sprintf("ratings_%d.csv", i+1)
			err := WriteCsvFile(outputFile, recordCh, numFiles, i)
			if err != nil {
				fmt.Printf("Error al escribir el archivo %s: %v\n", outputFile, err)
			}
		}(i)
	}

	// Leemos los registros en streaming y los enviamos al canal
	err := ReadRatingsCsvFile(ratingsFile, recordCh)
	if err != nil {
		return err
	}
	close(recordCh)
	wg.Wait()

	// Contamos los ratings por género y calculamos el promedio
	genresCount, genresAverage, err := CountRatingsByGenre(ratingsFile, moviesFile)
	if err != nil {
		return err
	}

	// Imprimimos los resultados
	for genre, count := range genresCount {
		avg := genresAverage[genre]
		fmt.Printf("Género: %s, Ratings: %d, Promedio: %.2f\n", genre, count, avg)
	}
	return nil
}

// ReadRatingsCsvFile lee el archivo CSV línea por línea y envía cada registro por el canal
func ReadRatingsCsvFile(filename string, recordCh chan<- recordWithIndex) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()
	reader := csv.NewReader(file)

	// Leemos la cabecera
	header, err := reader.Read()
	if err != nil {
		return err
	}
	// Enviamos la cabecera como el primer registro (índice -1) para que cada goroutine escriba la cabecera en su archivo
	recordCh <- recordWithIndex{record: header, index: -1}

	// Leemos y enviamos cada registro con su índice
	counter := 0
	for {
		record, err := reader.Read()
		if err != nil {
			break
		}
		recordCh <- recordWithIndex{record: record, index: counter}
		counter++
	}
	return nil
}

// WriteCsvFile escribe un archivo CSV con los registros asignados en función de un índice
func WriteCsvFile(filename string, recordCh <-chan recordWithIndex, numFiles, index int) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	defer writer.Flush()

	for recordInfo := range recordCh {
		if recordInfo.index == -1 {
			if err := writer.Write(recordInfo.record); err != nil {
				return err
			}
			continue
		}
		if recordInfo.index%numFiles == index {
			if err := writer.Write(recordInfo.record); err != nil {
				return err
			}
		}
	}
	return nil
}

// CountRatingsByGenre cuenta el número de ratings y calcula el promedio por género
func CountRatingsByGenre(ratingsFile, moviesFile string) (map[string]int, map[string]float64, error) {
	genresCount := make(map[string]int)
	genresSum := make(map[string]float64)

	// Leemos el archivo de películas y construimos un mapa de géneros
	movies, err := os.Open(moviesFile)
	if err != nil {
		return genresCount, genresSum, err
	}
	defer movies.Close()
	moviesReader := csv.NewReader(movies)
	_, err = moviesReader.Read() // Saltamos la cabecera
	if err != nil {
		return genresCount, genresSum, err
	}
	movieGenres := make(map[string][]string)
	for {
		record, err := moviesReader.Read()
		if err != nil {
			break
		}
		movieID := record[0]
		genres := strings.Split(record[2], "|")
		movieGenres[movieID] = genres
	}

	// Leemos el archivo de ratings y contamos por género
	ratings, err := os.Open(ratingsFile)
	if err != nil {
		return genresCount, genresSum, err
	}
	defer ratings.Close()
	ratingsReader := csv.NewReader(ratings)
	_, err = ratingsReader.Read() // Saltamos la cabecera
	if err != nil {
		return genresCount, genresSum, err
	}
	for {
		record, err := ratingsReader.Read()
		if err != nil {
			break
		}
		movieID := record[1]
		rating := record[2]
		ratingFloat, err := strconv.ParseFloat(rating, 64)
		if err != nil {
			return genresCount, genresSum, err
		}
		genres, ok := movieGenres[movieID]
		if ok {
			for _, genre := range genres {
				genresCount[genre]++
				genresSum[genre] += ratingFloat
			}
		}
	}

	genresAverage := make(map[string]float64)
	for genre, sum := range genresSum {
		genresAverage[genre] = sum / float64(genresCount[genre])
	}

	return genresCount, genresAverage, nil
}
