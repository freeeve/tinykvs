package main

import (
	"crypto/md5"
	"encoding/hex"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"time"

	"github.com/freeeve/tinykvs"
)

// Realistic data generators
var (
	firstNames = []string{
		"James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda", "William", "Elizabeth",
		"David", "Barbara", "Richard", "Susan", "Joseph", "Jessica", "Thomas", "Sarah", "Charles", "Karen",
		"Christopher", "Nancy", "Daniel", "Lisa", "Matthew", "Betty", "Anthony", "Margaret", "Mark", "Sandra",
		"Donald", "Ashley", "Steven", "Kimberly", "Paul", "Emily", "Andrew", "Donna", "Joshua", "Michelle",
		"Kenneth", "Dorothy", "Kevin", "Carol", "Brian", "Amanda", "George", "Melissa", "Timothy", "Deborah",
		"Ronald", "Stephanie", "Edward", "Rebecca", "Jason", "Sharon", "Jeffrey", "Laura", "Ryan", "Cynthia",
		"Jacob", "Kathleen", "Gary", "Amy", "Nicholas", "Angela", "Eric", "Shirley", "Jonathan", "Anna",
		"Stephen", "Brenda", "Larry", "Pamela", "Justin", "Emma", "Scott", "Nicole", "Brandon", "Helen",
		"Benjamin", "Samantha", "Samuel", "Katherine", "Raymond", "Christine", "Gregory", "Debra", "Frank", "Rachel",
		"Alexander", "Carolyn", "Patrick", "Janet", "Jack", "Catherine", "Dennis", "Maria", "Jerry", "Heather",
	}
	lastNames = []string{
		"Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez",
		"Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
		"Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson",
		"Walker", "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores",
		"Green", "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell", "Carter", "Roberts",
		"Gomez", "Phillips", "Evans", "Turner", "Diaz", "Parker", "Cruz", "Edwards", "Collins", "Reyes",
		"Stewart", "Morris", "Morales", "Murphy", "Cook", "Rogers", "Gutierrez", "Ortiz", "Morgan", "Cooper",
		"Peterson", "Bailey", "Reed", "Kelly", "Howard", "Ramos", "Kim", "Cox", "Ward", "Richardson",
		"Watson", "Brooks", "Chavez", "Wood", "James", "Bennett", "Gray", "Mendoza", "Ruiz", "Hughes",
		"Price", "Alvarez", "Castillo", "Sanders", "Patel", "Myers", "Long", "Ross", "Foster", "Jimenez",
	}
	domains = []string{"gmail.com", "yahoo.com", "outlook.com", "icloud.com", "proton.me", "hotmail.com", "aol.com", "mail.com"}
	// Top 100 US cities with their states
	cityStates = []struct{ city, state string }{
		{"New York", "NY"}, {"Los Angeles", "CA"}, {"Chicago", "IL"}, {"Houston", "TX"}, {"Phoenix", "AZ"},
		{"Philadelphia", "PA"}, {"San Antonio", "TX"}, {"San Diego", "CA"}, {"Dallas", "TX"}, {"San Jose", "CA"},
		{"Austin", "TX"}, {"Jacksonville", "FL"}, {"Fort Worth", "TX"}, {"Columbus", "OH"}, {"Charlotte", "NC"},
		{"San Francisco", "CA"}, {"Indianapolis", "IN"}, {"Seattle", "WA"}, {"Denver", "CO"}, {"Washington", "DC"},
		{"Boston", "MA"}, {"El Paso", "TX"}, {"Nashville", "TN"}, {"Detroit", "MI"}, {"Oklahoma City", "OK"},
		{"Portland", "OR"}, {"Las Vegas", "NV"}, {"Memphis", "TN"}, {"Louisville", "KY"}, {"Baltimore", "MD"},
		{"Milwaukee", "WI"}, {"Albuquerque", "NM"}, {"Tucson", "AZ"}, {"Fresno", "CA"}, {"Sacramento", "CA"},
		{"Mesa", "AZ"}, {"Kansas City", "MO"}, {"Atlanta", "GA"}, {"Long Beach", "CA"}, {"Colorado Springs", "CO"},
		{"Raleigh", "NC"}, {"Miami", "FL"}, {"Virginia Beach", "VA"}, {"Omaha", "NE"}, {"Oakland", "CA"},
		{"Minneapolis", "MN"}, {"Tulsa", "OK"}, {"Arlington", "TX"}, {"New Orleans", "LA"}, {"Wichita", "KS"},
		{"Cleveland", "OH"}, {"Tampa", "FL"}, {"Bakersfield", "CA"}, {"Aurora", "CO"}, {"Honolulu", "HI"},
		{"Anaheim", "CA"}, {"Santa Ana", "CA"}, {"Corpus Christi", "TX"}, {"Riverside", "CA"}, {"Lexington", "KY"},
		{"St. Louis", "MO"}, {"Stockton", "CA"}, {"Pittsburgh", "PA"}, {"Saint Paul", "MN"}, {"Anchorage", "AK"},
		{"Cincinnati", "OH"}, {"Henderson", "NV"}, {"Greensboro", "NC"}, {"Plano", "TX"}, {"Newark", "NJ"},
		{"Lincoln", "NE"}, {"Toledo", "OH"}, {"Orlando", "FL"}, {"Chula Vista", "CA"}, {"Irvine", "CA"},
		{"Fort Wayne", "IN"}, {"Jersey City", "NJ"}, {"Durham", "NC"}, {"St. Petersburg", "FL"}, {"Laredo", "TX"},
		{"Buffalo", "NY"}, {"Madison", "WI"}, {"Lubbock", "TX"}, {"Chandler", "AZ"}, {"Scottsdale", "AZ"},
		{"Glendale", "AZ"}, {"Reno", "NV"}, {"Norfolk", "VA"}, {"Winston-Salem", "NC"}, {"North Las Vegas", "NV"},
		{"Gilbert", "AZ"}, {"Irving", "TX"}, {"Hialeah", "FL"}, {"Garland", "TX"}, {"Fremont", "CA"},
		{"Boise", "ID"}, {"Richmond", "VA"}, {"Baton Rouge", "LA"}, {"Spokane", "WA"}, {"Des Moines", "IA"},
	}
)

func generateUserID(i int) string {
	// Create a hash-based ID that looks like a UUID
	h := md5.Sum([]byte(fmt.Sprintf("user-%d-salt", i)))
	return hex.EncodeToString(h[:])[:24]
}

var streetTypes = []string{"St", "Ave", "Blvd", "Dr", "Ln", "Rd", "Way", "Ct", "Pl", "Cir"}
var streetNames = []string{"Main", "Oak", "Maple", "Cedar", "Pine", "Elm", "Washington", "Lake", "Hill", "Park", "River", "Church", "High", "Union", "Market", "Spring", "School", "North", "South", "West"}

func generateUser(rng *rand.Rand, i int) (string, string) {
	userID := generateUserID(i)
	key := fmt.Sprintf("user:%s", userID)

	firstName := firstNames[rng.Intn(len(firstNames))]
	lastName := lastNames[rng.Intn(len(lastNames))]
	loc := cityStates[rng.Intn(len(cityStates))]
	age := 18 + rng.Intn(62)
	balance := rng.Float64() * 10000
	streetNum := 100 + rng.Intn(9900)
	streetName := streetNames[rng.Intn(len(streetNames))]
	streetType := streetTypes[rng.Intn(len(streetTypes))]
	zip := 10000 + rng.Intn(90000)

	// Generate 3-7 lucky numbers
	numLucky := 3 + rng.Intn(5)
	luckyNums := make([]int, numLucky)
	for j := 0; j < numLucky; j++ {
		luckyNums[j] = 1 + rng.Intn(99)
	}
	luckyStr := fmt.Sprintf("%v", luckyNums)
	luckyStr = luckyStr[1 : len(luckyStr)-1] // remove [ ]
	luckyStr = "[" + luckyStr + "]"

	value := fmt.Sprintf(`{"id":"%s","name":"%s %s","email":"%s.%s@%s","age":%d,"balance":%.2f,"active":%v,"created":%d,"lucky_numbers":%s,"address":{"street":"%d %s %s","city":"%s","state":"%s","zip":"%05d"}}`,
		userID,
		firstName, lastName,
		firstName, lastName, domains[rng.Intn(len(domains))],
		age,
		balance,
		rng.Intn(2) == 1,
		1700000000+i,
		luckyStr,
		streetNum, streetName, streetType,
		loc.city, loc.state,
		zip,
	)
	return key, value
}

func main() {
	numRecords := flag.Int("records", 100_000_000, "Number of records to write")
	numReads := flag.Int("reads", 100_000, "Number of reads to perform per test")
	dataDir := flag.String("dir", "/tmp/tinykvs-bench", "Data directory")
	skipWrite := flag.Bool("skip-write", false, "Skip write phase (use existing data)")
	skipCompact := flag.Bool("skip-compact", false, "Skip compaction phase")
	memtableSize := flag.Int64("memtable", 4*1024*1024, "Memtable size in bytes (default 4MB)")
	blockSize := flag.Int("block-size", 16*1024, "Block size in bytes (default 16KB)")
	compression := flag.String("compression", "zstd", "Compression type: zstd, snappy, none")
	disableBloom := flag.Bool("no-bloom", true, "Disable bloom filters (default true for low memory)")
	cpuProfile := flag.String("cpuprofile", "", "Write CPU profile to file")
	memProfile := flag.String("memprofile", "", "Write memory profile to file")
	flag.Parse()

	// Parse compression type
	var compressionType tinykvs.CompressionType
	switch *compression {
	case "snappy":
		compressionType = tinykvs.CompressionSnappy
	case "none":
		compressionType = tinykvs.CompressionNone
	default:
		compressionType = tinykvs.CompressionZstd
	}

	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Could not create CPU profile: %v\n", err)
			os.Exit(1)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			fmt.Fprintf(os.Stderr, "Could not start CPU profile: %v\n", err)
			os.Exit(1)
		}
		defer pprof.StopCPUProfile()
	}

	if *memProfile != "" {
		defer func() {
			f, err := os.Create(*memProfile)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Could not create memory profile: %v\n", err)
				return
			}
			defer f.Close()
			runtime.GC()
			if err := pprof.WriteHeapProfile(f); err != nil {
				fmt.Fprintf(os.Stderr, "Could not write memory profile: %v\n", err)
			}
		}()
	}

	fmt.Println("=== TinyKVS Benchmark ===")
	fmt.Printf("Platform: %s/%s\n", runtime.GOOS, runtime.GOARCH)
	fmt.Printf("GOMEMLIMIT: %d bytes\n", debug.SetMemoryLimit(-1))
	fmt.Printf("Records: %d\n", *numRecords)
	fmt.Printf("Memtable: %d MB\n", *memtableSize/1024/1024)
	fmt.Printf("Block size: %d KB\n", *blockSize/1024)
	fmt.Printf("Compression: %s\n", *compression)
	fmt.Printf("Bloom filters: %v\n", !*disableBloom)
	fmt.Printf("Data dir: %s\n", *dataDir)
	fmt.Println()

	// Configure options for low memory
	opts := tinykvs.LowMemoryOptions(*dataDir)
	opts.MemtableSize = *memtableSize
	opts.BlockSize = *blockSize
	opts.CompressionType = compressionType
	opts.DisableBloomFilter = *disableBloom
	opts.WALSyncMode = tinykvs.WALSyncNone

	if !*skipWrite {
		runWrite(*dataDir, opts, *numRecords)
	}

	// Read before compaction
	fmt.Println("\n=== READ BEFORE COMPACTION ===")
	runReads(*dataDir, opts, *numRecords, *numReads)

	// Prefix scan before compaction
	fmt.Println("\n=== PREFIX SCAN BEFORE COMPACTION ===")
	runPrefixScans(*dataDir, opts, *numRecords)

	if !*skipCompact {
		runCompact(*dataDir, opts)
	}

	// Read after compaction
	fmt.Println("\n=== READ AFTER COMPACTION ===")
	runReads(*dataDir, opts, *numRecords, *numReads)

	// Prefix scan after compaction
	fmt.Println("\n=== PREFIX SCAN AFTER COMPACTION ===")
	runPrefixScans(*dataDir, opts, *numRecords)

	fmt.Println("\n=== BENCHMARK COMPLETE ===")
}

func runWrite(dir string, opts tinykvs.Options, numRecords int) {
	fmt.Println("=== WRITE PHASE ===")

	// Clean up old data
	os.RemoveAll(dir)

	store, err := tinykvs.Open(dir, opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open store: %v\n", err)
		os.Exit(1)
	}

	batchSize := 1_000_000
	writeStart := time.Now()
	lastReport := writeStart
	rng := rand.New(rand.NewSource(42)) // Deterministic for reproducibility

	for i := 0; i < numRecords; i++ {
		key, value := generateUser(rng, i)
		if err := store.PutString([]byte(key), value); err != nil {
			fmt.Fprintf(os.Stderr, "Put failed at %d: %v\n", i, err)
			os.Exit(1)
		}

		if (i+1)%batchSize == 0 {
			elapsed := time.Since(lastReport)
			totalElapsed := time.Since(writeStart)
			rate := float64(batchSize) / elapsed.Seconds()
			avgRate := float64(i+1) / totalElapsed.Seconds()
			pct := float64(i+1) / float64(numRecords) * 100

			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			storeStats := store.Stats()

			fmt.Printf("[%s] Written: %dM / %dM (%.1f%%) | Batch: %.0f/s | Avg: %.0f/s | Heap: %dMB | Sys: %dMB | Idx: %dMB\n",
				totalElapsed.Truncate(time.Second), (i+1)/1_000_000, numRecords/1_000_000, pct,
				rate, avgRate, m.HeapAlloc/1024/1024, m.Sys/1024/1024, storeStats.IndexMemory/1024/1024)

			lastReport = time.Now()

			// Periodic GC to keep memory in check
			if (i+1)%(10*batchSize) == 0 {
				runtime.GC()
				debug.FreeOSMemory()
			}
		}
	}

	// Final flush
	fmt.Println("Flushing...")
	flushStart := time.Now()
	if err := store.Flush(); err != nil {
		fmt.Fprintf(os.Stderr, "Flush failed: %v\n", err)
	}
	fmt.Printf("Flush completed in %v\n", time.Since(flushStart))

	writeDuration := time.Since(writeStart)
	writeRate := float64(numRecords) / writeDuration.Seconds()
	fmt.Printf("\nWrite complete: %d records in %v (%.0f ops/sec)\n",
		numRecords, writeDuration, writeRate)

	// Print stats
	stats := store.Stats()
	for i, level := range stats.Levels {
		if level.NumTables > 0 {
			fmt.Printf("  L%d: %d tables, %d keys, %d bytes\n",
				i, level.NumTables, level.NumKeys, level.Size)
		}
	}

	store.Close()
}

func runCompact(dir string, opts tinykvs.Options) {
	fmt.Println("\n=== COMPACTION PHASE ===")

	store, err := tinykvs.Open(dir, opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open store: %v\n", err)
		return
	}

	compactStart := time.Now()
	fmt.Println("Compacting L0 -> L1...")

	if err := store.Compact(); err != nil {
		fmt.Printf("Compaction error: %v\n", err)
	}

	fmt.Printf("Compaction completed in %v\n", time.Since(compactStart))

	// Print stats
	stats := store.Stats()
	for i, level := range stats.Levels {
		if level.NumTables > 0 {
			fmt.Printf("  L%d: %d tables, %d keys, %d bytes\n",
				i, level.NumTables, level.NumKeys, level.Size)
		}
	}

	store.Close()

	// GC after compaction
	runtime.GC()
	debug.FreeOSMemory()
}

func runReads(dir string, opts tinykvs.Options, numRecords, numReads int) {
	cacheSizes := []struct {
		name string
		size int64
	}{
		{"0MB", 0},
		{"64MB", 64 * 1024 * 1024},
	}

	for _, cs := range cacheSizes {
		opts.BlockCacheSize = cs.size

		store, err := tinykvs.Open(dir, opts)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to open store: %v\n", err)
			continue
		}

		// Random reads
		readStart := time.Now()
		found := 0
		for i := 0; i < numReads; i++ {
			idx := rand.Intn(numRecords)
			key := fmt.Sprintf("user:%s", generateUserID(idx))
			if _, err := store.Get([]byte(key)); err == nil {
				found++
			}
		}
		readDuration := time.Since(readStart)
		readRate := float64(numReads) / readDuration.Seconds()

		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		storeStats := store.Stats()

		fmt.Printf("Cache %s: %d reads in %v (%.0f/s) | Found: %d | Heap: %dMB | Sys: %dMB | Idx: %dMB\n",
			cs.name, numReads, readDuration, readRate, found,
			m.HeapAlloc/1024/1024, m.Sys/1024/1024, storeStats.IndexMemory/1024/1024)

		store.Close()
	}
}

func runPrefixScans(dir string, opts tinykvs.Options, numRecords int) {
	store, err := tinykvs.Open(dir, opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open store: %v\n", err)
		return
	}
	defer store.Close()

	// Full scan of all keys
	scanStart := time.Now()
	count := 0
	err = store.ScanPrefix([]byte("user:"), func(key []byte, value tinykvs.Value) bool {
		count++
		return true
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "ScanPrefix failed: %v\n", err)
		return
	}
	scanDuration := time.Since(scanStart)
	scanRate := float64(count) / scanDuration.Seconds()

	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	storeStats := store.Stats()

	fmt.Printf("Full scan: %d keys in %v (%.0f keys/s) | Heap: %dMB | Sys: %dMB | Idx: %dMB\n",
		count, scanDuration, scanRate,
		m.HeapAlloc/1024/1024, m.Sys/1024/1024, storeStats.IndexMemory/1024/1024)

	// Scan 1000 random prefixes to measure per-prefix overhead
	// Use 2-char hex prefix (matches ~1/256 of keys)
	numPrefixScans := 1000
	totalKeys := 0
	scanStart = time.Now()
	for i := 0; i < numPrefixScans; i++ {
		// Pick a random 2-char hex prefix
		prefix := fmt.Sprintf("user:%02x", rand.Intn(256))
		store.ScanPrefix([]byte(prefix), func(key []byte, value tinykvs.Value) bool {
			totalKeys++
			return true
		})
	}
	scanDuration = time.Since(scanStart)
	avgKeysPerPrefix := float64(totalKeys) / float64(numPrefixScans)
	prefixRate := float64(numPrefixScans) / scanDuration.Seconds()

	runtime.ReadMemStats(&m)
	storeStats = store.Stats()
	fmt.Printf("Random prefix scans: %d scans in %v (%.0f scans/s, avg %.1f keys/scan) | Heap: %dMB | Sys: %dMB | Idx: %dMB\n",
		numPrefixScans, scanDuration, prefixRate, avgKeysPerPrefix,
		m.HeapAlloc/1024/1024, m.Sys/1024/1024, storeStats.IndexMemory/1024/1024)

	// Scan with LIMIT to show lazy loading benefit
	// Use 1-char hex prefix (matches ~1/16 of keys) but stop at limit
	numLimitScans := 1000
	limit := 100
	totalLimitKeys := 0
	scanStart = time.Now()
	for i := 0; i < numLimitScans; i++ {
		prefix := fmt.Sprintf("user:%x", rand.Intn(16))
		count := 0
		store.ScanPrefix([]byte(prefix), func(key []byte, value tinykvs.Value) bool {
			count++
			totalLimitKeys++
			return count < limit // Stop after limit
		})
	}
	scanDuration = time.Since(scanStart)
	avgLimitKeys := float64(totalLimitKeys) / float64(numLimitScans)
	limitRate := float64(numLimitScans) / scanDuration.Seconds()

	runtime.ReadMemStats(&m)
	storeStats = store.Stats()
	fmt.Printf("LIMIT %d scans: %d scans in %v (%.0f scans/s, avg %.1f keys/scan) | Heap: %dMB | Sys: %dMB | Idx: %dMB\n",
		limit, numLimitScans, scanDuration.Truncate(time.Millisecond), limitRate, avgLimitKeys,
		m.HeapAlloc/1024/1024, m.Sys/1024/1024, storeStats.IndexMemory/1024/1024)
}
