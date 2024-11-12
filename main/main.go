package main

import (
	"bufio"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type StationStats struct {
	min   float64
	max   float64
	sum   float64
	count int64
}

func (s *StationStats) addMeasurement(temp float64) {
	if temp < s.min || s.count == 0 {
		s.min = temp
	}
	if temp > s.max || s.count == 0 {
		s.max = temp
	}
	s.sum += temp
	s.count++
}

func (s StationStats) String() string {
	return fmt.Sprintf("%.1f/%.1f/%.1f",
		s.min,
		s.sum/float64(s.count),
		s.max,
	)
}

// printMemUsage outputs the current memory usage
func printMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

func processChunk(chunk []string, results map[string]*StationStats, mutex *sync.Mutex) {
	localStats := make(map[string]*StationStats)

	for _, line := range chunk {
		parts := strings.Split(line, ";")
		if len(parts) != 2 {
			continue
		}
		
		station := parts[0]
		temp, err := strconv.ParseFloat(parts[1], 64)
		if err != nil {
			continue
		}

		stats, exists := localStats[station]
		if !exists {
			stats = &StationStats{
				min:   temp,
				max:   temp,
				sum:   0,
				count: 0,
			}
			localStats[station] = stats
		}
		stats.addMeasurement(temp)
	}

	mutex.Lock()
	for station, localStat := range localStats {
		globalStat, exists := results[station]
		if !exists {
			results[station] = localStat
		} else {
			if localStat.min < globalStat.min {
				globalStat.min = localStat.min
			}
			if localStat.max > globalStat.max {
				globalStat.max = localStat.max
			}
			globalStat.sum += localStat.sum
			globalStat.count += localStat.count
		}
	}
	mutex.Unlock()
}

func main() {
	// Start timing
	startTime := time.Now()
	
	// Print initial memory stats
	fmt.Println("Initial memory stats:")
	printMemUsage()

	file, err := os.Open("measurements_B.txt")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	numWorkers := runtime.NumCPU()
	fmt.Printf("\nUsing %d worker goroutines\n", numWorkers)
	
	chunkSize := 100000
	chunks := make(chan []string)
	
	var wg sync.WaitGroup
	results := make(map[string]*StationStats)
	var mutex sync.Mutex

	// Start measurement counter
	var totalMeasurements int64

	// Start workers
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for chunk := range chunks {
				processChunk(chunk, results, &mutex)
			}
		}()
	}

	// Process file
	scanner := bufio.NewScanner(file)
	currentChunk := make([]string, 0, chunkSize)
	
	for scanner.Scan() {
		currentChunk = append(currentChunk, scanner.Text())
		totalMeasurements++
		
		if len(currentChunk) == chunkSize {
			chunks <- currentChunk
			currentChunk = make([]string, 0, chunkSize)

			// Print progress every 10 million rows
			if totalMeasurements%10_000_000 == 0 {
				fmt.Printf("Processed %d million measurements...\n", totalMeasurements/1_000_000)
				printMemUsage()
			}
		}
	}
	
	if len(currentChunk) > 0 {
		chunks <- currentChunk
	}
	
	close(chunks)
	wg.Wait()

	// Sort and print results
	stations := make([]string, 0, len(results))
	for station := range results {
		stations = append(stations, station)
	}
	sort.Strings(stations)

	fmt.Println("\nResults:")
	for _, station := range stations {
		fmt.Printf("%s=%v\n", station, results[station])
	}

	// Print final stats
	duration := time.Since(startTime)
	fmt.Printf("\nProcessing completed in: %v\n", duration)
	fmt.Printf("Total measurements processed: %d\n", totalMeasurements)
	fmt.Printf("Processing speed: %.2f million measurements/second\n", 
		float64(totalMeasurements)/(duration.Seconds()*1_000_000))
	
	fmt.Println("\nFinal memory stats:")
	printMemUsage()
}