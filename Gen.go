package main

import (
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"time"
)

// Signature base para los generadores
type KeyGenFunc func(length int) ([]byte, error)

// Estructura de métricas basada en primitivas
type BenchResult struct {
	TotalGenerated  int
	TotalCollisions int
	Duration        time.Duration
	KeysPerSecond   float64
	MemAllocatedMB  float64
}

// Generador pseudo-aleatorio rápido basado en arrays y operaciones de módulo nativas
func FastKeyGen(length int) ([]byte, error) {
	const charset = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
	key := make([]byte, length)
	
	for i := 0; i < length; i++ {
		key[i] = charset[rand.Intn(len(charset))]
	}
	return key, nil
}

// Orquestador del banco de pruebas (Mecanismo Fan-In con canales con buffer)
func RunTestBench(keygen KeyGenFunc, totalKeys int, keyLength int, concurrency int) BenchResult {
	// Canal con buffer para desacoplar la generación del análisis de colisiones
	resultsChan := make(chan []byte, 8192)
	var wg sync.WaitGroup
	
	keysPerWorker := totalKeys / concurrency
	startTime := time.Now()

	// Inyectores de carga (Workers concurrentes)
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < keysPerWorker; j++ {
				key, err := keygen(keyLength)
				if err == nil {
					resultsChan <- key
				}
			}
		}()
	}

	// Monitor para cerrar el canal de forma segura al terminar las goroutines
	go func() {
		wg.Wait()
		close(resultsChan)
	}()

	// Consumidor Único: Procesa el mapa de colisiones de forma secuencial y segura
	// Pre-asignamos la capacidad del mapa para mitigar el overhead del re-hashing en memoria
	registry := make(map[string]bool, totalKeys) 
	collisions := 0
	totalProcessed := 0

	for key := range resultsChan {
		totalProcessed++
		if registry[string(key)] {
			collisions++
		} else {
			registry[string(key)] = true
		}
	}

	duration := time.Since(startTime)
	
	// Captura del estado del Garbage Collector y memoria del sistema
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	return BenchResult{
		TotalGenerated:  totalProcessed,
		TotalCollisions: collisions,
		Duration:        duration,
		KeysPerSecond:   float64(totalProcessed) / duration.Seconds(),
		MemAllocatedMB:  float64(m.Alloc) / 1024 / 1024,
	}
}

func main() {
	// Forzamos el uso de todos los núcleos lógicos asignados por el entorno virtual
	cores := runtime.NumCPU()
	runtime.GOMAXPROCS(cores)

	// Configuración de estrés del banco de pruebas
	totalKeys := 1_000_000 // 1 Millón de llaves para una prueba rápida y eficiente
	keyLength := 12        // 12 caracteres de longitud por clave
	workers := cores * 2   // Multiplicador clásico para exprimir el planificador de Go

	fmt.Println("================================================================")
	fmt.Println("   BANCO DE PRUEBAS PARA GENERADOR DE CLAVES (KERNEL GO)        ")
	fmt.Println("================================================================")
	fmt.Printf("Núcleos CPU Detectados : %d\n", cores)
	fmt.Printf("Workers Concurrentes   : %d\n", workers)
	fmt.Printf("Claves a Generar       : %d\n", totalKeys)
	fmt.Printf("Longitud de Clave      : %d bytes\n", keyLength)
	fmt.Println("----------------------------------------------------------------")
	fmt.Println("Ejecutando prueba de esfuerzo...")

	// Ejecución del testbench
	result := RunTestBench(FastKeyGen, totalKeys, keyLength, workers)

	// Reporte final por consola
	fmt.Println("\n=== RESULTADOS FINALES ===")
	fmt.Printf("Tiempo de Ejecución     : %v\n", result.Duration)
	fmt.Printf("Rendimiento (Thpt)      : %.2f ops/sec\n", result.KeysPerSecond)
	fmt.Printf("Memoria Heap en uso     : %.2f MB\n", result.MemAllocatedMB)
	fmt.Printf("Colisiones Detectadas   : %d\n", result.TotalCollisions)
	
	collisionRate := (float64(result.TotalCollisions) / float64(result.TotalGenerated)) * 100
	fmt.Printf("Tasa de Inseguridad     : %.6f%%\n", collisionRate)
	fmt.Println("================================================================")
}
