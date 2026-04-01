# AI SQL Schema Assistant 🚀

A local AI-powered SQL assistant that understands complex database schemas and generates optimized SQL queries using rules and examples.

## 🔥 Features

* 🔍 Automatic database schema extraction (tables, columns, relations)
* 🧠 Intelligent schema analysis (detects possible joins and relationships)
* 📜 Rule-based query generation (custom business logic)
* 📚 Example-based learning (few-shot prompting)
* 🔒 Fully local AI (no data leaves your machine)
* ⚡ Works with MSSQL + Ollama + DeepSeek-Coder

---

## 🧠 How It Works

1. Extracts database schema from MSSQL
2. Converts schema into AI-readable format
3. Applies custom rules and examples
4. Sends prompt to local LLM (Ollama)
5. Generates SQL queries

---

## 🛠️ Tech Stack

* Python
* MSSQL (SQL Server)
* Ollama (local LLM runtime)
* DeepSeek-Coder (6.7B)

---

## ⚙️ Setup

### 1. Install Ollama

Download from: https://ollama.com

```bash
ollama run deepseek-coder:6.7b
```

---

### 2. Install dependencies

```bash
pip install pyodbc requests
```

---

### 3. Configure database connection

Edit connection string in code:

```python
SERVER=localhost,1433
DATABASE=Northwind
UID=sa
PWD=YourPassword
```

---

### 4. Add rules & examples

Edit `rules.json`:

* Define business rules
* Add correct SQL examples

---

### 5. Run project

```bash
python main.py
```

---

## 🧪 Example

Input:

```
"Müşterilerin siparişlerini getir"
```

Output:

```sql
SELECT c.CompanyName, o.OrderID
FROM Customers c
JOIN Orders o ON c.CustomerID = o.CustomerID
```

---

## 💡 Why This Project?

Modern databases are complex and poorly documented.
This tool helps developers:

* Understand messy schemas
* Avoid wrong joins
* Generate correct SQL faster
* Apply business rules automatically

---

## 🔮 Future Improvements

* Query validation
* Auto-learning from feedback
* Spring Boot integration
* UI dashboard

---

## 📌 Author

Ramazan Bozkurt
