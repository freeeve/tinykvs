#!/bin/bash
# EC2 Benchmark Script for TinyKVS
# Provisions a t4g.micro instance (1GB RAM), runs benchmarks with swap
#
# Prerequisites:
# - AWS CLI configured with credentials
# - SSH key pair created in AWS
#
# Usage: ./ec2_benchmark.sh [num_records] [key_name]
# Example: ./ec2_benchmark.sh 1000000000 my-key-pair
#
# For 1B records on t4g.micro:
# - Uses 4GB swap for safety
# - GOMEMLIMIT=700MiB to leave room for OS
# - LowMemoryOptions (1MB memtable, no bloom, no cache)

set -e

NUM_RECORDS=${1:-100000000}  # Default 100M records
KEY_NAME=${2:-""}
INSTANCE_TYPE="t4g.micro"
AMI_ID=""  # Will be set based on region
REGION=$(aws configure get region || echo "us-east-1")

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log() {
    echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1"
    exit 1
}

warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

# Get latest Amazon Linux 2023 ARM AMI
get_ami_id() {
    log "Finding latest Amazon Linux 2023 ARM AMI in $REGION..."
    AMI_ID=$(aws ec2 describe-images \
        --owners amazon \
        --filters "Name=name,Values=al2023-ami-*-arm64" \
                  "Name=state,Values=available" \
        --query "Images | sort_by(@, &CreationDate) | [-1].ImageId" \
        --output text \
        --region "$REGION")

    if [ -z "$AMI_ID" ] || [ "$AMI_ID" == "None" ]; then
        error "Could not find AMI"
    fi
    log "Using AMI: $AMI_ID"
}

# Create security group if needed
create_security_group() {
    SG_NAME="tinykvs-benchmark-sg"

    # Check if exists
    SG_ID=$(aws ec2 describe-security-groups \
        --filters "Name=group-name,Values=$SG_NAME" \
        --query "SecurityGroups[0].GroupId" \
        --output text \
        --region "$REGION" 2>/dev/null || echo "None")

    if [ "$SG_ID" == "None" ] || [ -z "$SG_ID" ]; then
        log "Creating security group..."
        SG_ID=$(aws ec2 create-security-group \
            --group-name "$SG_NAME" \
            --description "TinyKVS Benchmark Security Group" \
            --region "$REGION" \
            --output text)

        # Add SSH rule
        aws ec2 authorize-security-group-ingress \
            --group-id "$SG_ID" \
            --protocol tcp \
            --port 22 \
            --cidr 0.0.0.0/0 \
            --region "$REGION"
    fi

    log "Using security group: $SG_ID"
}

# Create user data script for instance initialization
create_user_data() {
    cat << 'USERDATA'
#!/bin/bash
set -e

# Setup 4GB swap for safety on 1GB instance
echo "Setting up swap..."
dd if=/dev/zero of=/swapfile bs=1M count=4096
chmod 600 /swapfile
mkswap /swapfile
swapon /swapfile
echo '/swapfile swap swap defaults 0 0' >> /etc/fstab
swapon --show

# Install Go
curl -LO https://go.dev/dl/go1.22.0.linux-arm64.tar.gz
tar -C /usr/local -xzf go1.22.0.linux-arm64.tar.gz
export PATH=$PATH:/usr/local/go/bin
echo 'export PATH=$PATH:/usr/local/go/bin' >> /home/ec2-user/.bashrc

# Install git
yum install -y git

# Clone tinykvs (update with your repo)
cd /home/ec2-user
git clone https://github.com/freeeve/tinykvs.git
cd tinykvs
chown -R ec2-user:ec2-user /home/ec2-user/tinykvs

# Create benchmark script
cat << 'BENCHMARK' > /home/ec2-user/run_benchmark.sh
#!/bin/bash
export PATH=$PATH:/usr/local/go/bin
export GOMEMLIMIT=700MiB
export GOGC=50

cd /home/ec2-user/tinykvs

NUM_RECORDS=${1:-1000000000}

echo "=== TinyKVS Benchmark on $(uname -m) ==="
echo "Records: $NUM_RECORDS"
echo "Memory limit: $GOMEMLIMIT"
echo "Swap:"
swapon --show
free -h
echo ""

# Create the benchmark binary
cat << 'BENCHCODE' > /tmp/bench.go
package main

import (
    "flag"
    "fmt"
    "math/rand"
    "os"
    "runtime"
    "runtime/debug"
    "time"

    "github.com/freeeve/tinykvs"
)

func main() {
    numRecords := flag.Int("records", 1000000000, "Number of records")
    numReads := flag.Int("reads", 100000, "Number of reads per test")
    dataDir := flag.String("dir", "/home/ec2-user/data", "Data directory")
    skipWrite := flag.Bool("skip-write", false, "Skip write phase")
    skipCompact := flag.Bool("skip-compact", false, "Skip compaction")
    flag.Parse()

    fmt.Println("=== TinyKVS Billion Record Benchmark ===")
    fmt.Printf("Platform: %s/%s\n", runtime.GOOS, runtime.GOARCH)
    fmt.Printf("GOMEMLIMIT: %v\n", debug.SetMemoryLimit(-1))
    fmt.Printf("Records: %d\n", *numRecords)
    fmt.Println()

    opts := tinykvs.LowMemoryOptions(*dataDir)
    opts.WALSyncMode = tinykvs.WALSyncNone

    if !*skipWrite {
        runWrite(*dataDir, opts, *numRecords)
    }

    fmt.Println("\n=== READ BEFORE COMPACTION ===")
    runReads(*dataDir, opts, *numRecords, *numReads)

    if !*skipCompact {
        runCompact(*dataDir, opts)
    }

    fmt.Println("\n=== READ AFTER COMPACTION ===")
    runReads(*dataDir, opts, *numRecords, *numReads)

    fmt.Println("\n=== COMPLETE ===")
}

func runWrite(dir string, opts tinykvs.Options, numRecords int) {
    fmt.Println("=== WRITE PHASE ===")
    os.RemoveAll(dir)
    os.MkdirAll(dir, 0755)

    store, err := tinykvs.Open(dir, opts)
    if err != nil {
        fmt.Fprintf(os.Stderr, "Open failed: %v\n", err)
        os.Exit(1)
    }

    batchSize := 1000000
    writeStart := time.Now()
    lastReport := writeStart

    for i := 0; i < numRecords; i++ {
        key := fmt.Sprintf("key%012d", i)
        value := fmt.Sprintf("val%012d", i)
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

            fmt.Printf("Written: %dM / %dM (%.1f%%) | Batch: %.0f/s | Avg: %.0f/s | Heap: %dMB | Sys: %dMB\n",
                (i+1)/1000000, numRecords/1000000, pct,
                rate, avgRate, m.HeapAlloc/1024/1024, m.Sys/1024/1024)

            lastReport = time.Now()

            if (i+1)%(10*batchSize) == 0 {
                runtime.GC()
                debug.FreeOSMemory()
            }
        }
    }

    fmt.Println("Flushing...")
    store.Flush()

    writeDuration := time.Since(writeStart)
    fmt.Printf("Write complete: %d records in %v (%.0f ops/sec)\n",
        numRecords, writeDuration, float64(numRecords)/writeDuration.Seconds())

    stats := store.Stats()
    for i, level := range stats.Levels {
        if level.NumTables > 0 {
            fmt.Printf("  L%d: %d tables, %d keys\n", i, level.NumTables, level.NumKeys)
        }
    }
    store.Close()
}

func runCompact(dir string, opts tinykvs.Options) {
    fmt.Println("\n=== COMPACTION PHASE ===")
    store, _ := tinykvs.Open(dir, opts)
    start := time.Now()
    store.Compact()
    fmt.Printf("Compaction complete in %v\n", time.Since(start))
    stats := store.Stats()
    for i, level := range stats.Levels {
        if level.NumTables > 0 {
            fmt.Printf("  L%d: %d tables, %d keys\n", i, level.NumTables, level.NumKeys)
        }
    }
    store.Close()
}

func runReads(dir string, opts tinykvs.Options, numRecords, numReads int) {
    for _, cacheSize := range []int64{0, 64 * 1024 * 1024} {
        opts.BlockCacheSize = cacheSize
        cacheName := "0MB"
        if cacheSize > 0 {
            cacheName = fmt.Sprintf("%dMB", cacheSize/1024/1024)
        }

        store, _ := tinykvs.Open(dir, opts)
        start := time.Now()
        found := 0
        for i := 0; i < numReads; i++ {
            key := fmt.Sprintf("key%012d", rand.Intn(numRecords))
            if _, err := store.Get([]byte(key)); err == nil {
                found++
            }
        }
        elapsed := time.Since(start)

        var m runtime.MemStats
        runtime.ReadMemStats(&m)
        fmt.Printf("Cache %s: %d reads in %v (%.0f/s) | Found: %d | Heap: %dMB\n",
            cacheName, numReads, elapsed, float64(numReads)/elapsed.Seconds(), found, m.HeapAlloc/1024/1024)

        store.Close()
    }
}
BENCHCODE

# Build and run
go build -o /tmp/tinykvs-bench /tmp/bench.go
/tmp/tinykvs-bench -records=$NUM_RECORDS 2>&1 | tee /home/ec2-user/benchmark_results.txt
BENCHMARK

chmod +x /home/ec2-user/run_benchmark.sh
chown ec2-user:ec2-user /home/ec2-user/run_benchmark.sh

# Signal ready
touch /home/ec2-user/ready
USERDATA
}

# Launch EC2 instance
launch_instance() {
    log "Launching $INSTANCE_TYPE instance..."

    USER_DATA=$(create_user_data | base64)

    KEY_OPTION=""
    if [ -n "$KEY_NAME" ]; then
        KEY_OPTION="--key-name $KEY_NAME"
    fi

    INSTANCE_ID=$(aws ec2 run-instances \
        --image-id "$AMI_ID" \
        --instance-type "$INSTANCE_TYPE" \
        --security-group-ids "$SG_ID" \
        --user-data "$USER_DATA" \
        $KEY_OPTION \
        --block-device-mappings '[{"DeviceName":"/dev/xvda","Ebs":{"VolumeSize":100,"VolumeType":"gp3"}}]' \
        --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=tinykvs-benchmark}]" \
        --region "$REGION" \
        --query "Instances[0].InstanceId" \
        --output text)

    log "Instance launched: $INSTANCE_ID"

    # Wait for instance to be running
    log "Waiting for instance to be running..."
    aws ec2 wait instance-running --instance-ids "$INSTANCE_ID" --region "$REGION"

    # Get public IP
    PUBLIC_IP=$(aws ec2 describe-instances \
        --instance-ids "$INSTANCE_ID" \
        --query "Reservations[0].Instances[0].PublicIpAddress" \
        --output text \
        --region "$REGION")

    log "Instance is running at: $PUBLIC_IP"

    echo "$INSTANCE_ID" > /tmp/tinykvs_instance_id
    echo "$PUBLIC_IP" > /tmp/tinykvs_instance_ip
}

# Wait for initialization and run benchmark
run_benchmark() {
    PUBLIC_IP=$(cat /tmp/tinykvs_instance_ip)

    log "Waiting for instance initialization (this may take a few minutes)..."

    # Wait for cloud-init to complete
    if [ -n "$KEY_NAME" ]; then
        for i in {1..60}; do
            if ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5 \
                -i ~/.ssh/${KEY_NAME}.pem ec2-user@$PUBLIC_IP \
                "test -f /home/ec2-user/ready" 2>/dev/null; then
                break
            fi
            sleep 10
            echo -n "."
        done
        echo ""

        log "Instance ready! Starting benchmark..."

        # Run benchmark
        ssh -o StrictHostKeyChecking=no \
            -i ~/.ssh/${KEY_NAME}.pem ec2-user@$PUBLIC_IP \
            "NUM_RECORDS=$NUM_RECORDS /home/ec2-user/run_benchmark.sh"

        # Download results
        log "Downloading results..."
        scp -o StrictHostKeyChecking=no \
            -i ~/.ssh/${KEY_NAME}.pem \
            ec2-user@$PUBLIC_IP:/home/ec2-user/benchmark_results.txt \
            ./benchmark_results_ec2.txt

        log "Results saved to benchmark_results_ec2.txt"
    else
        warn "No SSH key provided. Connect manually to $PUBLIC_IP to run benchmark."
        warn "Run: /home/ec2-user/run_benchmark.sh $NUM_RECORDS"
    fi
}

# Cleanup
cleanup() {
    if [ -f /tmp/tinykvs_instance_id ]; then
        INSTANCE_ID=$(cat /tmp/tinykvs_instance_id)
        read -p "Terminate instance $INSTANCE_ID? (y/n) " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            log "Terminating instance..."
            aws ec2 terminate-instances --instance-ids "$INSTANCE_ID" --region "$REGION"
            rm -f /tmp/tinykvs_instance_id /tmp/tinykvs_instance_ip
            log "Instance terminated"
        fi
    fi
}

# Main
main() {
    log "TinyKVS EC2 Benchmark"
    log "Records: $NUM_RECORDS"
    log "Instance type: $INSTANCE_TYPE"
    log "Region: $REGION"
    echo ""

    if [ -z "$KEY_NAME" ]; then
        warn "No SSH key specified. You'll need to connect via Session Manager or specify a key."
        warn "Usage: $0 <num_records> <key_name>"
    fi

    get_ami_id
    create_security_group
    launch_instance
    run_benchmark
    cleanup
}

# Handle termination
trap cleanup EXIT

main "$@"
