#!/bin/bash
# =============================================================================
# OMOP CDM Lakehouse Deployment Script
# =============================================================================
# This script deploys the AWS infrastructure for the healthcare lakehouse.
# Prerequisites:
#   - AWS CLI configured with appropriate credentials
#   - Terraform >= 1.0.0 installed
#   - S3 bucket for Terraform state (optional, but recommended)
# =============================================================================

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TERRAFORM_DIR="${SCRIPT_DIR}/../infrastructure/terraform"
ETL_SCRIPTS_DIR="${SCRIPT_DIR}/../etl/jobs"

# Default values
ENVIRONMENT="dev"
AWS_REGION="us-east-1"
PROJECT_NAME="omop-cdm-lakehouse"

# =============================================================================
# Helper Functions
# =============================================================================

print_header() {
    echo -e "\n${GREEN}============================================${NC}"
    echo -e "${GREEN}$1${NC}"
    echo -e "${GREEN}============================================${NC}\n"
}

print_warning() {
    echo -e "${YELLOW}WARNING: $1${NC}"
}

print_error() {
    echo -e "${RED}ERROR: $1${NC}"
}

print_success() {
    echo -e "${GREEN}SUCCESS: $1${NC}"
}

check_prerequisite() {
    if ! command -v $1 &> /dev/null; then
        print_error "$1 is required but not installed."
        exit 1
    fi
    echo "✓ $1 is installed"
}

# =============================================================================
# Pre-flight Checks
# =============================================================================

preflight_checks() {
    print_header "Running Pre-flight Checks"
    
    check_prerequisite "aws"
    check_prerequisite "terraform"
    
    # Check AWS credentials
    echo "Checking AWS credentials..."
    if aws sts get-caller-identity &> /dev/null; then
        ACCOUNT_ID=$(aws sts get-caller-identity --query "Account" --output text)
        echo "✓ AWS credentials configured (Account: $ACCOUNT_ID)"
    else
        print_error "AWS credentials not configured or invalid"
        exit 1
    fi
    
    # Check Terraform version
    TF_VERSION=$(terraform version -json | python3 -c "import sys, json; print(json.load(sys.stdin)['terraform_version'])")
    echo "✓ Terraform version: $TF_VERSION"
}

# =============================================================================
# Terraform Deployment
# =============================================================================

deploy_infrastructure() {
    print_header "Deploying Infrastructure with Terraform"
    
    cd "$TERRAFORM_DIR"
    
    # Initialize Terraform
    echo "Initializing Terraform..."
    terraform init
    
    # Plan
    echo "Creating Terraform plan..."
    terraform plan \
        -var="environment=${ENVIRONMENT}" \
        -var="aws_region=${AWS_REGION}" \
        -var="project_name=${PROJECT_NAME}" \
        -out=tfplan
    
    # Confirm before apply
    read -p "Do you want to apply this plan? (yes/no): " CONFIRM
    if [[ "$CONFIRM" != "yes" ]]; then
        echo "Deployment cancelled."
        exit 0
    fi
    
    # Apply
    echo "Applying Terraform plan..."
    terraform apply tfplan
    
    # Get outputs
    echo ""
    echo "Terraform outputs:"
    terraform output
    
    # Save outputs for scripts
    terraform output -json > terraform_outputs.json
    
    print_success "Infrastructure deployed successfully!"
}

# =============================================================================
# Upload ETL Scripts
# =============================================================================

upload_etl_scripts() {
    print_header "Uploading ETL Scripts to S3"
    
    # Get S3 bucket from Terraform outputs
    S3_BUCKET=$(terraform -chdir="$TERRAFORM_DIR" output -raw data_lake_bucket_name 2>/dev/null || echo "")
    
    if [[ -z "$S3_BUCKET" ]]; then
        print_error "Could not determine S3 bucket from Terraform outputs"
        exit 1
    fi
    
    echo "Uploading to s3://${S3_BUCKET}/scripts/"
    
    # Upload each ETL script
    for script in "$ETL_SCRIPTS_DIR"/*.py; do
        if [[ -f "$script" ]]; then
            filename=$(basename "$script")
            echo "  Uploading $filename..."
            aws s3 cp "$script" "s3://${S3_BUCKET}/scripts/${filename}"
        fi
    done
    
    print_success "ETL scripts uploaded successfully!"
}

# =============================================================================
# Create Sample Data Directories
# =============================================================================

create_data_directories() {
    print_header "Creating S3 Data Directories"
    
    S3_BUCKET=$(terraform -chdir="$TERRAFORM_DIR" output -raw data_lake_bucket_name 2>/dev/null || echo "")
    
    if [[ -z "$S3_BUCKET" ]]; then
        print_error "Could not determine S3 bucket from Terraform outputs"
        exit 1
    fi
    
    # Create empty files to establish directory structure
    directories=(
        "raw/synthea/"
        "raw/vocabulary/"
        "bronze/"
        "silver/clinical/"
        "silver/vocabulary/"
        "silver/health_system/"
        "silver/economics/"
        "silver/derived/"
        "silver/etl_staging/"
        "gold/"
    )
    
    for dir in "${directories[@]}"; do
        echo "  Creating $dir..."
        echo "" | aws s3 cp - "s3://${S3_BUCKET}/${dir}.keep" --content-type "text/plain"
    done
    
    print_success "Data directories created!"
}

# =============================================================================
# Run Initial Setup Pipeline
# =============================================================================

run_initial_setup() {
    print_header "Running Initial Setup Pipeline"
    
    STATE_MACHINE_ARN=$(terraform -chdir="$TERRAFORM_DIR" output -raw initial_setup_state_machine_arn 2>/dev/null || echo "")
    
    if [[ -z "$STATE_MACHINE_ARN" ]]; then
        print_warning "Initial setup state machine not found. Skipping."
        return
    fi
    
    echo "Starting initial setup state machine..."
    EXECUTION_ARN=$(aws stepfunctions start-execution \
        --state-machine-arn "$STATE_MACHINE_ARN" \
        --input '{"source": "deployment_script"}' \
        --query "executionArn" --output text)
    
    echo "Execution started: $EXECUTION_ARN"
    echo ""
    echo "Monitor progress in AWS Console or run:"
    echo "  aws stepfunctions describe-execution --execution-arn $EXECUTION_ARN"
}

# =============================================================================
# Cleanup (Optional)
# =============================================================================

cleanup() {
    print_header "Cleaning Up Resources"
    
    cd "$TERRAFORM_DIR"
    
    read -p "This will DESTROY all resources. Are you sure? (yes/no): " CONFIRM
    if [[ "$CONFIRM" != "yes" ]]; then
        echo "Cleanup cancelled."
        exit 0
    fi
    
    terraform destroy \
        -var="environment=${ENVIRONMENT}" \
        -var="aws_region=${AWS_REGION}" \
        -var="project_name=${PROJECT_NAME}"
    
    print_success "Resources destroyed!"
}

# =============================================================================
# Main
# =============================================================================

usage() {
    echo "Usage: $0 [command] [options]"
    echo ""
    echo "Commands:"
    echo "  deploy      Deploy all infrastructure and upload scripts"
    echo "  infra       Deploy infrastructure only"
    echo "  scripts     Upload ETL scripts only"
    echo "  setup       Run initial setup pipeline"
    echo "  destroy     Destroy all resources"
    echo ""
    echo "Options:"
    echo "  -e, --env       Environment (dev/staging/prod), default: dev"
    echo "  -r, --region    AWS region, default: us-east-1"
    echo "  -p, --project   Project name, default: omop-cdm-lakehouse"
    echo ""
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -e|--env)
            ENVIRONMENT="$2"
            shift 2
            ;;
        -r|--region)
            AWS_REGION="$2"
            shift 2
            ;;
        -p|--project)
            PROJECT_NAME="$2"
            shift 2
            ;;
        deploy|infra|scripts|setup|destroy)
            COMMAND="$1"
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

# Execute command
case $COMMAND in
    deploy)
        preflight_checks
        deploy_infrastructure
        upload_etl_scripts
        create_data_directories
        print_success "Deployment complete!"
        ;;
    infra)
        preflight_checks
        deploy_infrastructure
        ;;
    scripts)
        upload_etl_scripts
        ;;
    setup)
        run_initial_setup
        ;;
    destroy)
        cleanup
        ;;
    *)
        usage
        ;;
esac
