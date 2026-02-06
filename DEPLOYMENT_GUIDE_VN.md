# Hướng dẫn Triển khai Dự án OMOP CDM Lakehouse

Dự án này triển khai **OMOP CDM v5.4** trên AWS Lakehouse với kiến trúc **Medallion (Bronze → Silver → Gold)** gồm 7 Job ETL chạy trên AWS Glue và Step Functions.

---

## 1. Chuẩn bị (Prerequisites)

| Yêu cầu | Mô tả |
|---------|-------|
| **AWS Account** | Đã cấu hình AWS CLI (`aws configure`) |
| **Terraform** | Phiên bản >= 1.0.0 |
| **Dữ liệu Synthea** | File CSV từ [Synthea](https://synthetichealth.github.io/synthea/) |
| **Vocabulary** | Tải từ [OHDSI Athena](https://athena.ohdsi.org/) (>5GB) |

---

## 2. Triển khai Hạ tầng (Infrastructure)

### 2.1. Cấu hình Terraform

Tạo file `infrastructure/terraform/terraform.tfvars`:
```hcl
aws_region         = "ap-southeast-1"
project_name       = "omop-cdm-lakehouse"
environment        = "dev"
glue_database_name = "healthcare"
```

### 2.2. Deploy

```bash
cd infrastructure/terraform

# Khởi tạo
terraform init

# Xem kế hoạch
terraform plan

# Triển khai (nhập "yes" khi được hỏi)
terraform apply
```

**Output quan trọng:**
- `s3_bucket_name`: Tên bucket chính
- `glue_role_arn`: Role cho Glue jobs
- `step_functions_arn`: ARN của pipelines

---

## 3. Upload Dữ liệu nguồn

```bash
# Lấy tên bucket từ output
BUCKET=$(terraform output -raw s3_bucket_name)

# Upload Synthea CSV
aws s3 sync ./synthea-output/ s3://$BUCKET/raw/synthea/

# Upload Vocabulary (CONCEPT.csv, CONCEPT_RELATIONSHIP.csv, ...)
aws s3 sync ./vocabulary/ s3://$BUCKET/raw/vocabulary/
```

---

## 4. Khởi chạy Pipeline

### 4.1. Phase 1: Initial Setup (Một lần duy nhất)

Load 33+ bảng Vocabulary vào Silver layer:

```bash
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:ap-southeast-1:YOUR_ACCOUNT:stateMachine:omop-cdm-lakehouse-initial-setup
```

> ⏱️ Thời gian: ~15-30 phút (tùy kích thước vocabulary)

### 4.2. Phase 2: ETL Pipeline (Định kỳ)

Xử lý Synthea → OMOP CDM:

```bash
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:ap-southeast-1:YOUR_ACCOUNT:stateMachine:omop-cdm-lakehouse-etl-pipeline
```

**Thứ tự Jobs:**

```
RawToBronze → ValidateBronze
     ↓
   Job 0 (Load Views)
     ↓
   Job 1 (Visit Grouping) → ValidateVisits
     ↓
   Job 2 (Person & Observation) → ValidatePerson
     ↓
   Job 3 (Visit Occurrence) → ValidateVisitOccurrence
     ↓
   ┌──────────────────┐
   │ Job 4 (Clinical) │──→ ValidateClinical
   │ Job 5 (Drugs)    │──→ ValidateDrugs
   └──────────────────┘
           ↓
   Job 6 (Eras & Metadata) → ValidateFinal
           ↓
         ✅ DONE
```

> ⏱️ Thời gian: ~45-90 phút (tùy data size)

---

## 5. Kiểm tra Kết quả

### 5.1. Truy vấn bằng Athena

```sql
-- Chọn Database: healthcare

-- Đếm số bệnh nhân
SELECT COUNT(*) as total_persons FROM person;

-- Đếm số lượt khám
SELECT COUNT(*) as total_visits FROM visit_occurrence;

-- Xem distribution condition
SELECT 
  c.concept_name,
  COUNT(*) as count
FROM condition_occurrence co
JOIN concept c ON co.condition_concept_id = c.concept_id
GROUP BY c.concept_name
ORDER BY count DESC
LIMIT 20;
```

### 5.2. Danh sách Tables trong Database `healthcare`

| Table | Mô tả |
|-------|-------|
| **Clinical Domain** | |
| `person` | Thông tin bệnh nhân |
| `observation_period` | Khoảng thời gian quan sát |
| `visit_occurrence` | Thông tin lượt khám |
| `condition_occurrence` | Chẩn đoán |
| `procedure_occurrence` | Thủ thuật |
| `drug_exposure` | Thuốc |
| `measurement` | Xét nghiệm (số) |
| `observation` | Quan sát (text) |
| **Derived** | |
| `condition_era` | Giai đoạn bệnh (≤30 ngày gap) |
| `drug_era` | Giai đoạn dùng thuốc |
| **Metadata** | |
| `cdm_source` | Thông tin nguồn |
| **Vocabulary** | |
| `concept`, `concept_relationship`, `vocabulary`, ... | 33+ bảng từ điển |

---

## 6. Monitoring & Troubleshooting

### 6.1. Xem trạng thái Pipeline
- **Step Functions Console**: Visualize từng bước, xem failed step
- **CloudWatch Logs**: `/aws-glue/jobs/output` - chi tiết execution

### 6.2. Lỗi thường gặp

| Lỗi | Nguyên nhân | Giải pháp |
|-----|------------|-----------|
| `Table not found` | Vocabulary chưa load | Chạy lại Initial Setup |
| `ValidationError` | Data quality issues | Check logs, có thể bỏ qua WARNING |
| `AccessDenied` | Lake Formation permissions | Grant quyền cho Glue role |
| `404 S3` | Stale Iceberg metadata | Xóa table trong Glue Catalog, chạy lại |

### 6.3. Re-run sau khi fix

```bash
# Xóa tables bị lỗi (nếu cần)
aws glue delete-table --database-name healthcare --name TABLE_NAME

# Start lại pipeline
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:ap-southeast-1:YOUR_ACCOUNT:stateMachine:omop-cdm-lakehouse-etl-pipeline
```

---

## 7. Clean Up (Xóa tài nguyên)

```bash
cd infrastructure/terraform
terraform destroy
```

> ⚠️ **Cảnh báo**: Lệnh này sẽ xóa TẤT CẢ tài nguyên bao gồm cả dữ liệu trong S3!

---

## Tham khảo

- [OMOP CDM Documentation](https://ohdsi.github.io/CommonDataModel/)
- [Apache Iceberg](https://iceberg.apache.org/)
- [AWS Glue Iceberg](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-iceberg.html)
