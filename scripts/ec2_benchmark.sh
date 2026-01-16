#!/bin/bash
# EC2 Benchmark Script for TinyKVS
# Provisions a t4g.micro instance (1GB RAM), runs benchmarks with swap
#
# Prerequisites:
# - AWS CLI configured with credentials
# - SSH key pair created in AWS
#
# Usage: ./ec2_benchmark.sh [num_records] [key_name] [compression]
# Example: ./ec2_benchmark.sh 1000000000 my-key-pair zstd
# Example: ./ec2_benchmark.sh 1000000000 "" snappy  # No SSH key, use SSM
#
# For 1B records on t4g.micro:
# - Uses 4GB swap for safety
# - GOMEMLIMIT=700MiB to leave room for OS
# - LowMemoryOptions (1MB memtable, no bloom, no cache)

set -e

NUM_RECORDS=${1:-100000000}  # Default 100M records
KEY_NAME=${2:-""}
COMPRESSION=${3:-"zstd"}     # Compression type: zstd, snappy, none
INSTANCE_TYPE="t4g.micro"
AMI_ID=""  # Will be set based on region
REGION=$(aws configure get region || echo "us-east-1")
S3_BUCKET="deep-libby"
S3_PREFIX="tinykvs-bench"

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

# Build benchmark binary for ARM64
build_benchmark() {
    BUILD_DIR=$(mktemp -d)
    BENCH_BINARY="$BUILD_DIR/tinykvs-bench"

    # Cross-compile for ARM64 Linux
    GOOS=linux GOARCH=arm64 go build -o "$BENCH_BINARY" ./cmd/tinykvs-bench

    if [ ! -f "$BENCH_BINARY" ]; then
        error "Failed to build benchmark binary"
    fi

    # Make it executable
    chmod +x "$BENCH_BINARY"

    echo "$BENCH_BINARY"
}

# Upload files to S3
upload_to_s3() {
    local bench_binary=$1
    local script_file=$2
    
    log "Uploading files to s3://$S3_BUCKET/$S3_PREFIX/..."
    
    # Upload benchmark binary
    aws s3 cp "$bench_binary" "s3://$S3_BUCKET/$S3_PREFIX/tinykvs-bench" \
        --region "$REGION" || error "Failed to upload benchmark binary"
    
    # Upload benchmark script
    aws s3 cp "$script_file" "s3://$S3_BUCKET/$S3_PREFIX/run_benchmark.sh" \
        --region "$REGION" || error "Failed to upload benchmark script"
    
    log "Files uploaded successfully"
}

# Create benchmark script file
create_benchmark_script() {
    local script_file=$(mktemp)
    
    cat << 'BENCHMARK_SCRIPT' > "$script_file"
#!/bin/bash

export PATH=$PATH:/usr/local/go/bin
export GOMEMLIMIT=700MiB
export GOGC=50

cd /home/ec2-user

NUM_RECORDS=${1:-1000000000}
COMPRESSION=${2:-"zstd"}
LOG_FILE="/home/ec2-user/bench.log"
ERR_FILE="/home/ec2-user/bench.err"
STATUS_FILE="/home/ec2-user/bench.status"

# Cleanup function to log exit
cleanup() {
    EXIT_CODE=${1:-$?}
    SIGNAL=$2
    {
        echo ""
        echo "=== PROCESS EXITED ==="
        echo "Exit code: $EXIT_CODE"
        if [ -n "$SIGNAL" ]; then
            echo "Signal: $SIGNAL"
        fi
        echo "Time: $(date)"
    } >> "$LOG_FILE" 2>&1
    echo "$EXIT_CODE" > "$STATUS_FILE" 2>&1
    return $EXIT_CODE
}

# Trap signals
trap 'cleanup 130 SIGTERM; exit 130' TERM
trap 'cleanup 130 SIGINT; exit 130' INT
trap 'cleanup $? EXIT' EXIT

echo "=== TinyKVS Benchmark on $(uname -m) ===" | tee -a "$LOG_FILE"
echo "Records: $NUM_RECORDS" | tee -a "$LOG_FILE"
echo "Compression: $COMPRESSION" | tee -a "$LOG_FILE"
echo "Memory limit: $GOMEMLIMIT" | tee -a "$LOG_FILE"
echo "Swap:" | tee -a "$LOG_FILE"
swapon --show | tee -a "$LOG_FILE"
free -h | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"

echo "Starting benchmark at $(date)" | tee -a "$LOG_FILE"

# Run benchmark with nohup to survive SSM session disconnects
# This ensures the process continues even if the SSM session times out
nohup /home/ec2-user/tinykvs-bench -records=$NUM_RECORDS -compression=$COMPRESSION -dir=/home/ec2-user/data >> "$LOG_FILE" 2>> "$ERR_FILE" &
BENCH_PID=$!

# Save PID for later checking
echo $BENCH_PID > /home/ec2-user/bench.pid

{
    echo "Benchmark started with PID: $BENCH_PID"
    echo "Process will continue even if SSM session disconnects (using nohup)"
    echo ""
    echo "To monitor progress: tail -f $LOG_FILE"
    echo "To check if running: ps -p $BENCH_PID"
    echo "To check exit code: cat $STATUS_FILE (after completion)"
    echo ""
    echo "You can safely disconnect from SSM - the process will continue running."
    echo "Reconnect later and check the log file for results."
} | tee -a "$LOG_FILE"

# Try to wait for completion (will fail if SSM disconnects, but process continues)
# If SSM session stays connected, we'll capture the exit code
# If it disconnects, the process continues and we can check status file later
if wait $BENCH_PID 2>/dev/null; then
    EXIT_CODE=$?
else
    # Wait failed (likely SSM disconnected), but process is still running
    # Check if it's actually still running
    if ps -p $BENCH_PID > /dev/null 2>&1; then
        echo "SSM session disconnected, but benchmark process is still running (PID: $BENCH_PID)" | tee -a "$LOG_FILE"
        echo "Reconnect and check status later" | tee -a "$LOG_FILE"
        exit 0  # Exit successfully - process is running
    else
        # Process finished while we weren't waiting, check status file
        if [ -f "$STATUS_FILE" ]; then
            EXIT_CODE=$(cat "$STATUS_FILE")
        else
            EXIT_CODE=1  # Unknown - process died without writing status
        fi
    fi
fi

{
    echo ""
    echo "=== BENCHMARK FINISHED ==="
    echo "Exit code: $EXIT_CODE"
    echo "Time: $(date)"
} | tee -a "$LOG_FILE"

if [ $EXIT_CODE -ne 0 ]; then
    {
        echo "Benchmark failed with exit code $EXIT_CODE"
        echo "Check $ERR_FILE for error details"
    } | tee -a "$LOG_FILE"
fi

cleanup $EXIT_CODE
exit $EXIT_CODE
BENCHMARK_SCRIPT

    chmod +x "$script_file"
    echo "$script_file"
}

# Create user data script for instance initialization
create_user_data() {
    cat << 'USERDATA'
#!/bin/bash
set -e

# Disable Docker/containerd to free up memory (not needed for benchmark)
systemctl stop docker containerd 2>/dev/null || true
systemctl disable docker containerd 2>/dev/null || true

# Setup 4GB swap for safety on 1GB instance
echo "Setting up swap..."
dd if=/dev/zero of=/swapfile bs=1M count=4096
chmod 600 /swapfile
mkswap /swapfile
swapon /swapfile
echo '/swapfile swap swap defaults 0 0' >> /etc/fstab
swapon --show

# Install AWS CLI if not present
if ! command -v aws &> /dev/null; then
    yum install -y aws-cli
fi

# Download benchmark binary and script from S3
cd /home/ec2-user
aws s3 cp s3://deep-libby/tinykvs-bench/tinykvs-bench /home/ec2-user/tinykvs-bench
aws s3 cp s3://deep-libby/tinykvs-bench/run_benchmark.sh /home/ec2-user/run_benchmark.sh

# Make files executable
chmod +x /home/ec2-user/tinykvs-bench
chmod +x /home/ec2-user/run_benchmark.sh
chown ec2-user:ec2-user /home/ec2-user/tinykvs-bench
chown ec2-user:ec2-user /home/ec2-user/run_benchmark.sh

# Verify downloads succeeded
if [ ! -f /home/ec2-user/tinykvs-bench ]; then
    echo "ERROR: Failed to download tinykvs-bench from S3" >&2
    exit 1
fi

if [ ! -f /home/ec2-user/run_benchmark.sh ]; then
    echo "ERROR: Failed to download run_benchmark.sh from S3" >&2
    exit 1
fi

# Signal ready
touch /home/ec2-user/ready
USERDATA
}

# Main function - updated to build and upload before launching
main() {
    log "TinyKVS EC2 Benchmark"
    log "Records: $NUM_RECORDS"
    log "Compression: $COMPRESSION"
    log "Instance type: $INSTANCE_TYPE"
    log "Region: $REGION"
    echo ""

    if [ -z "$KEY_NAME" ]; then
        warn "No SSH key specified. You'll need to connect via Session Manager or specify a key."
        warn "Usage: $0 <num_records> <key_name> [compression]"
    fi

    # Build and upload before launching
    log "Building benchmark binary for ARM64..."
    BENCH_BINARY=$(build_benchmark)
    
    log "Creating benchmark script..."
    SCRIPT_FILE=$(create_benchmark_script)
    
    log "Uploading to S3..."
    upload_to_s3 "$BENCH_BINARY" "$SCRIPT_FILE"
    
    # Cleanup temp files
    rm -f "$BENCH_BINARY" "$SCRIPT_FILE"
    
    get_ami_id
    create_security_group
    launch_instance
    run_benchmark
    cleanup
}

# Check for existing instance
check_existing_instance() {
    EXISTING_ID=$(aws ec2 describe-instances \
        --filters "Name=tag:Name,Values=tinykvs-benchmark" \
                  "Name=instance-state-name,Values=running,stopping,stopped" \
        --query "Reservations[0].Instances[0].InstanceId" \
        --output text \
        --region "$REGION" 2>/dev/null || echo "None")
    
    if [ "$EXISTING_ID" != "None" ] && [ -n "$EXISTING_ID" ]; then
        log "Found existing instance: $EXISTING_ID"
        
        # Get its state
        STATE=$(aws ec2 describe-instances \
            --instance-ids "$EXISTING_ID" \
            --query "Reservations[0].Instances[0].State.Name" \
            --output text \
            --region "$REGION")
        
        if [ "$STATE" == "running" ]; then
            log "Instance is already running, reusing it"
            INSTANCE_ID="$EXISTING_ID"
            
            # Get public IP
            PUBLIC_IP=$(aws ec2 describe-instances \
                --instance-ids "$INSTANCE_ID" \
                --query "Reservations[0].Instances[0].PublicIpAddress" \
                --output text \
                --region "$REGION")
            
            echo "$INSTANCE_ID" > /tmp/tinykvs_instance_id
            echo "$PUBLIC_IP" > /tmp/tinykvs_instance_ip
            return 0
        elif [ "$STATE" == "stopped" ]; then
            log "Instance is stopped, starting it..."
            aws ec2 start-instances --instance-ids "$EXISTING_ID" --region "$REGION"
            aws ec2 wait instance-running --instance-ids "$EXISTING_ID" --region "$REGION"
            
            INSTANCE_ID="$EXISTING_ID"
            PUBLIC_IP=$(aws ec2 describe-instances \
                --instance-ids "$INSTANCE_ID" \
                --query "Reservations[0].Instances[0].PublicIpAddress" \
                --output text \
                --region "$REGION")
            
            echo "$INSTANCE_ID" > /tmp/tinykvs_instance_id
            echo "$PUBLIC_IP" > /tmp/tinykvs_instance_ip
            return 0
        fi
    fi
    return 1
}

# Launch EC2 instance
launch_instance() {
    # Check for existing instance first
    if check_existing_instance; then
        return 0
    fi
    
    log "No existing instance found, launching new $INSTANCE_TYPE instance..."

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
    INSTANCE_ID=$(cat /tmp/tinykvs_instance_id)
    PUBLIC_IP=$(cat /tmp/tinykvs_instance_ip)

    log "Waiting for instance initialization (this may take a few minutes)..."

    # Wait for cloud-init to complete or files to be ready
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
            "/home/ec2-user/run_benchmark.sh $NUM_RECORDS $COMPRESSION"

        # Download results
        log "Downloading results..."
        scp -o StrictHostKeyChecking=no \
            -i ~/.ssh/${KEY_NAME}.pem \
            ec2-user@$PUBLIC_IP:/home/ec2-user/benchmark_results.txt \
            ./benchmark_results_ec2.txt

        log "Results saved to benchmark_results_ec2.txt"
    else
        # Use SSM to run benchmark
        log "Using SSM to run benchmark on instance $INSTANCE_ID..."
        
        # Wait a bit for files to be downloaded from S3
        log "Waiting for S3 files to be downloaded..."
        sleep 10
        
        # Download files from S3 if not already there
        log "Ensuring benchmark files are on instance..."
        aws ssm send-command \
            --instance-ids "$INSTANCE_ID" \
            --document-name "AWS-RunShellScript" \
            --parameters "commands=[
                'aws s3 cp s3://deep-libby/tinykvs-bench/tinykvs-bench /home/ec2-user/tinykvs-bench || true',
                'aws s3 cp s3://deep-libby/tinykvs-bench/run_benchmark.sh /home/ec2-user/run_benchmark.sh || true',
                'chmod +x /home/ec2-user/tinykvs-bench /home/ec2-user/run_benchmark.sh || true',
                'test -f /home/ec2-user/tinykvs-bench && echo \"Files ready\" || echo \"Files not ready\"'
            ]" \
            --query "Command.CommandId" \
            --output text > /tmp/ssm_cmd_id
        
        sleep 5
        
        # Run the benchmark via SSM
        log "Starting benchmark via SSM..."
        BENCH_CMD_ID=$(aws ssm send-command \
            --instance-ids "$INSTANCE_ID" \
            --document-name "AWS-RunShellScript" \
            --parameters "commands=[\"/home/ec2-user/run_benchmark.sh $NUM_RECORDS $COMPRESSION\"]" \
            --query "Command.CommandId" \
            --output text)
        
        log "Benchmark started via SSM (Command ID: $BENCH_CMD_ID)"
        log "Monitor progress with: aws ssm get-command-invocation --command-id $BENCH_CMD_ID --instance-id $INSTANCE_ID"
        log "Or check logs on instance: tail -f /home/ec2-user/bench.log"
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

# Handle termination
trap cleanup EXIT

main "$@"
