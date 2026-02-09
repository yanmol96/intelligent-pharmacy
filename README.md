# Intelligent Pharmacy System 🏥💊

A production-style microservices-based pharmacy automation platform built using:

- FastAPI
- PostgreSQL
- RabbitMQ
- Redis
- MinIO (S3 Object Storage)
- Docker
- ML PDF Parsing

This system simulates a real-world digital pharmacy pipeline:
Doctor → ML extraction → Validation → Inventory → Billing → Returns → Accounting

---

# 🧠 System Architecture

Microservices:

- Order & Prescription Service
- ML Service (PDF extraction + drug detection)
- Inventory Service
- Pharmacy Service
- API Gateway
- PostgreSQL
- Redis
- RabbitMQ
- MinIO (Object Storage)

---

# 📦 Features Implemented

## Core Flow
- Upload prescription (PDF)
- Store file in Object Store (MinIO)
- ML extracts medicines
- Drug interaction detection
- Pharmacist review workflow
- Inventory reservation
- Order confirmation
- Billing engine
- Returns workflow
- Accounting entries

---

## Advanced Production Features

### Security
- JWT Authentication
- Role-based access:
  - DOCTOR
  - PHARMACIST
  - ADMIN

### Reliability
- Idempotency protection
- RabbitMQ event-driven architecture
- Redis caching

### Data Layer
- Medicine catalog
- Price catalog
- Audit logs
- Invoice history
- Accounting ledger

### Storage
- Raw prescription files stored in S3 (MinIO)

---

# 🚀 Running the System

## 1) Start all services
```bash
docker compose up --build
```

Services:

| Service | Port |
|---|---|
| API Gateway | 9000 |
| Pharmacy | 8001 |
| ML | 8002 |
| Order | 8003 |
| Inventory | 8004 |
| Postgres | 5432 |
| Redis | 6379 |
| RabbitMQ | 15672 |
| MinIO | 9001 |

---

# 🔐 Authentication

## Login
```
POST /auth/login
```

Example:
```bash
curl -X POST "http://localhost:9000/auth/login?username=doctor1&password=doctor123"
```

Response:
```
access_token
role
```

Use token:

```
Authorization: Bearer <token>
```

---

# 👨‍⚕️ DOCTOR APIs

## Upload Prescription
```
POST /prescriptions
```

```bash
curl -X POST http://localhost:9000/prescriptions  -H "Authorization: Bearer TOKEN"  -F "patient_name=Lakshya"  -F "file=@sample_prescription.pdf"
```

Stores file in MinIO + triggers ML.

---

# 🤖 ML Pipeline

Automatic:

- Download PDF from S3
- Extract text
- Detect medicines
- Check interactions
- Save results
- Update status

---

# 👩‍⚕️ PHARMACIST APIs

## Approve Prescription
```
POST /prescriptions/{id}/approve
```

## Reject Prescription
```
POST /prescriptions/{id}/reject
```

---

# 📦 ORDER APIs

## Create Order
```
POST /orders?prescription_id=ID
```

## Confirm Order
```
POST /orders/{prescription_id}/confirm
Headers:
Idempotency-Key: unique_key
```

---

# 💰 BILLING ENGINE

## Generate Bill
```
GET /orders/{order_id}/bill
```

Returns:
- Item totals
- GST
- Grand total

---

# 🔁 RETURNS

## Return Medicines
```
POST /orders/{order_id}/return
```

Rules:
- Allowed within 14 days
- Stock auto-restored
- Stored in returns table

---

# 📊 INVENTORY APIs

## View Inventory
```
GET /inventory
```

---

# 🧾 DATABASE TABLES

- prescriptions
- prescription_items
- medicines
- medicine_prices
- inventory
- orders
- returns
- invoices
- accounting_entries
- audit_logs
- users
- idempotency_keys

---

# 🧠 ML Behavior

Detects:
- Paracetamol
- Ibuprofen
- Amoxicillin
- Aspirin

Default quantity:
- Conservative estimate (6 tablets)

---

# 🪣 Object Storage (MinIO)

Stores:
- Raw prescription PDFs

Bucket:
```
prescriptions
```

---

# 🧪 Example End-to-End Flow

1) Doctor uploads prescription  
2) ML extracts medicines  
3) Interaction check  
4) Pharmacist approves  
5) Inventory reserved  
6) Order confirmed  
7) Bill generated  
8) Return processed  

---

# 🔮 Future Improvements

- Vision-based OCR model
- AI dosage extraction
- Auto-reorder stock
- Analytics dashboard
- Fraud detection

---

# 👨‍💻 Author

Lakshya Yadav
