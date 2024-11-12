package main

import (
	"bufio"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"time"
)

func main() {
	// Configure generation parameters
	stations := []string{
		"Hamburg", "Berlin", "Munich", "Frankfurt", "Stuttgart",
		"Dresden", "Leipzig", "Hannover", "Nuremberg", "Bremen",
	}

	filename := flag.String("create", "measurement.txt", "To create a text file")
	flag.Parse()

	file, err := os.Create(*filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	fmt.Print("Give a number: ")
	var numRows int // 1 million rows for testing (adjust as needed)
	fmt.Scanf("%d", &numRows)

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	// Seed the random number generator
	rand.Seed(time.Now().UnixNano())

	// Generate data
	for i := 0; i < numRows; i++ {
		station := stations[rand.Intn(len(stations))]
		// Generate temperature between -50°C and 50°C
		temp := rand.Float64()*100 - 50

		// Write to file with 1 decimal place precision
		_, err := fmt.Fprintf(writer, "%s;%.1f\n", station, temp)
		if err != nil {
			panic(err)
		}

		// Print progress every million rows
		if (i+1)%1_000_000 == 0 {
			fmt.Printf("Generated %d million rows\n", (i+1)/1_000_000)
		}
	}

	fmt.Println("Data generation complete!")
}
