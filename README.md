# OMOP CDM Healthcare Lakehouse on AWS

> **Modern ETL pipeline for OMOP Common Data Model v5.4 using Apache Iceberg, AWS Glue, and Step Functions**

[![AWS](https://img.shields.io/badge/AWS-Cloud-orange.svg)](https://aws.amazon.com/)
[![Iceberg](https://img.shields.io/badge/Apache-Iceberg-blue.svg)](https://iceberg.apache.org/)
[![OMOP CDM](https://img.shields.io/badge/OMOP-CDM%20v5.4-green.svg)](https://ohdsi.github.io/CommonDataModel/)
[![Terraform](https://img.shields.io/badge/IaC-Terraform-purple.svg)](https://www.terraform.io/)

---

## 📋 Overview

Pipeline chuyển đổi dữ liệu y tế thô (Synthea) sang chuẩn **OMOP CDM v5.4** trên AWS Lakehouse:

- ✅ **OMOP CDM v5.4**: Chuẩn hóa dữ liệu y tế quốc tế (OHDSI)
- ✅ **Apache Iceberg**: ACID transactions + Time Travel trên Data Lake
- ✅ **Serverless**: Chi phí thấp (Glue + Athena + S3)
- ✅ **Scalable**: Xử lý petabyte-scale data

---

## 🏗️ Architecture

### Data Flow

```
┌─────────────┐     ┌──────────────┐     ┌─────────────────────────────────┐
│   Synthea   │────▶│ Bronze Layer │────▶│         Silver Layer            │
│   (CSV)     │     │  (Parquet)   │     │   (OMOP CDM v5.4 - Iceberg)     │
└─────────────┘     └──────────────┘     └─────────────────────────────────┘
     Raw             Data Ingest              Standardized Healthcare Data
                                              
                                              ┌── person
                                              ├── visit_occurrence
                                              ├── condition_occurrence
                                              ├── drug_exposure
                                              ├── procedure_occurrence
                                              ├── measurement
                                              ├── observation
                                              ├── condition_era
                                              └── drug_era
```

### ETL Pipeline Structure

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      AWS Step Functions Orchestration                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  RawToBronze ─▶ Job0 ─▶ Job1 ─▶ Job2 ─▶ Job3 ─▶ [Job4 || Job5] ─▶ Job6    │
│      │           │        │        │        │         │              │      │
│      ▼           ▼        ▼        ▼        ▼         ▼              ▼      │
│  Validate    Bronze    Visit    Person   Visit    Clinical       Era &     │
│   Bronze     Views    Grouping   & Obs   Occur    & Drugs     Metadata    │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Technology Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| **Storage** | Amazon S3 | Raw, Bronze, Silver layers |
| **Table Format** | Apache Iceberg | ACID, Time Travel, Schema Evolution |
| **ETL Engine** | AWS Glue (Spark 3.3) | Data transformation |
| **Orchestration** | AWS Step Functions | Pipeline control & error handling |
| **Query Engine** | Amazon Athena | Serverless SQL analytics |
| **Metadata** | AWS Glue Catalog | Table registry |
| **IaC** | Terraform | Infrastructure provisioning |

---

## 🚀 Quick Start

### Prerequisites

- AWS Account với CLI (`aws configure`)
- Terraform >= 1.0.0
- Synthea data ([Download](https://synthetichealth.github.io/synthea/))
- OMOP Vocabulary ([Athena](https://athena.ohdsi.org/))

### 1. Deploy Infrastructure

```bash
cd infrastructure/terraform
terraform init && terraform apply
```

### 2. Upload Data

```bash
BUCKET=$(terraform output -raw s3_bucket_name)
aws s3 sync ./synthea-output/ s3://$BUCKET/raw/synthea/
aws s3 sync ./vocabulary/ s3://$BUCKET/raw/vocabulary/
```

### 3. Run Initial Setup (One-time)

```bash
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:REGION:ACCOUNT:stateMachine:omop-cdm-lakehouse-initial-setup
```

### 4. Run ETL Pipeline

```bash
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:REGION:ACCOUNT:stateMachine:omop-cdm-lakehouse-etl-pipeline
```

### 5. Query with Athena

```sql
SELECT COUNT(*) FROM healthcare.person;
SELECT * FROM healthcare.condition_occurrence LIMIT 10;
```

---

## 🔧 ETL Jobs

| Job | Description | Output |
|-----|-------------|--------|
| **RawToBronze** | Ingest CSV → Parquet + metadata | Bronze tables |
| **Job 0** | Create Spark views from Bronze | Temp views |
| **Job 1** | Group encounters → visits | `all_visits`, `visit_id_mapping` |
| **Job 2** | Extract demographics | `person`, `observation_period`, `location` |
| **Job 3** | Build visit occurrence | `visit_occurrence` |
| **Job 4** | Process clinical events | `condition_occurrence`, `procedure_occurrence`, `measurement`, `observation` |
| **Job 5** | Process medications | `drug_exposure` |
| **Job 6** | Calculate eras + metadata | `condition_era`, `drug_era`, `cdm_source` |

---

## 📊 Analytics

Pre-built queries for dashboards:

```bash
# See: analytics/dashboard_queries.sql
# Export to S3 for QuickSight/Tableau/PowerBI
```

**2 Dashboards:**
1. **Population Health Overview** - Demographics, visits, top diseases
2. **Clinical Analytics** - Cohorts, comorbidity, drug patterns

---

## 📁 Project Structure

```
omop-cdm/
├── infrastructure/terraform/    # AWS resources
├── etl/jobs/                    # 7 ETL + 7 validation jobs
├── analytics/                   # Dashboard queries
├── docs/                        # Technical docs
└── README.md
```

---

## 💡 Key Features

| Feature | Description |
|---------|-------------|
| **Time Travel** | `SELECT * FROM person TIMESTAMP AS OF '2026-01-31'` |
| **ACID** | Concurrent writes without corruption |
| **Cost** | ~$30-50/month (vs $600+ RDS) |
| **Validation** | Data quality checks at each step |

---

## 🛠️ Troubleshooting

- **Step Functions Console**: Visual pipeline status
- **CloudWatch Logs**: `/aws-glue/jobs/output`
- **Re-run**: `aws stepfunctions start-execution --state-machine-arn ARN`

---

## 📚 Documentation

- [Deployment Guide (VN)](DEPLOYMENT_GUIDE_VN.md)
- [Pipeline Flowcharts](docs/pipeline_flow.md)
- [Lakehouse vs Traditional DB](docs/LAKEHOUSE_VS_TRADITIONAL_DB.md)

---

## 🧹 Clean Up

```bash
terraform destroy  # ⚠️ Deletes all data!
```

---

## 📖 References

- [OMOP CDM](https://ohdsi.github.io/CommonDataModel/)
- [Apache Iceberg](https://iceberg.apache.org/)
- [Synthea](https://synthetichealth.github.io/synthea/)

---

**Built with ❤️ for healthcare analytics**
