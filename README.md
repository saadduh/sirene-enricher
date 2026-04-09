# 🚀 SIRENE Enricher v7.0

SIRENE Enricher is a high-performance Python tool designed to automate the enrichment of business datasets using the official **INSEE SIRENE API**. 

Whether you have a list of SIRET numbers in an Excel file that needs addresses, employee counts, or legal status, this tool handles the heavy lifting with a modern Desktop GUI and a robust Headless CLI mode.

---

## ✨ Key Features

* **Dual Interface**: Use the intuitive **Tkinter Desktop GUI** or the **Headless CLI** for automated workflows.
* **High Performance**: Multi-threaded processing (`SIRENEWorker`) ensures fast data fetching without freezing the interface.
* **Smart Resilience**:
    * **Auto-Checkpointing**: If the process is interrupted, it saves progress automatically. Resume exactly where you left off.
    * **Intelligent Retries**: Built-in handling for rate limits (HTTP 429) and network timeouts.
* **Advanced Data Cleaning**: Automatically maps obscure INSEE codes to human-readable labels (e.g., NAF codes, legal categories, and employee size brackets).
* **Global Caching**: Maintains a local cache (`.sirene_cache`) to avoid redundant API calls for previously enriched SIRETs.
* **Data Visualization**: Real-time stats and charts showing activity sectors (NAF) and regional distribution of your data.

---

## 🛠️ Installation

### 1. Prerequisites
* **Python 3.8+**
* **INSEE API Key**: Obtain your free key from the [Insee Developer Portal](https://portail-api.insee.fr/).

### 2. Setup
Clone the repository and install the required libraries:

```bash
git clone [https://github.com/saadduh/sirene-enricher.git](https://github.com/saadduh/sirene-enricher.git)
cd sirene-enricher
pip install requests pandas openpyxl Pillow matplotlib
```

---

## 🛠 How to Use

### 💻 Desktop Version (GUI)
Simply run the script to launch the interface:

```bash
python main.py
```

1.  **Paste** your INSEE API Key.
2.  **Select** your Excel File (ensure it contains a column named `SIRET`).
3.  **Click** `Start Enrichment`.

### ⚙️ Command Line Version (Headless)
Perfect for server environments or large-scale automation:

```bash
python main.py --headless --file data.xlsx --key YOUR_API_KEY --output enriched_results
```

#### 💡 Additional CLI Arguments
| Argument | Description | Default |
| :--- | :--- | :--- |
| `--delay` | Seconds between requests to avoid rate limits | `2.0` |
| `--batch-size` | Split output into smaller files (e.g., 50 rows/file) | `None` |
| `--col` | Change the target column name | `SIRET` |

---

## 📊 Data Enriched
The tool automatically appends the following verified information to your Excel file:

* **Denomination:** Official legal company name.
* **Legal Status:** Human-readable company type (SAS, SARL, etc.).
* **Workforce:** Employee range (e.g., "50 to 99 employees").
* **Activity:** NAF code and full category description.
* **Full Address:** Number, street, city, postal code, and region.
* **IDCC:** Collective agreement identifier (IDCC code).

---

## 📁 Project Structure
```text
├── main.py             # Core application engine and GUI logic
├── setup.iss           # Configuration for the Inno Setup installer
├── .sirene_cache/      # Auto-generated session checkpoints & global cache
└── README.md           # Project documentation
```

---

## 📝 License
Distributed under the **MIT License**. See `LICENSE` for more information.

---

## 👨‍💻 Author
**Saad Janina**
*Data Analyst & Digital Transformation Specialist*

---
> [!TIP]
> Ensure your `SIRET` column is formatted as text in Excel to avoid scientific notation errors!
