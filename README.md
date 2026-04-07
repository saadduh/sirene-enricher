🚀 SIRENE Enricher v7.0
SIRENE Enricher is a high-performance Python tool designed to automate the enrichment of business datasets using the official INSEE SIRENE API.

Whether you have a list of SIRET numbers in an Excel file that needs addresses, employee counts, or legal status, this tool handles the heavy lifting with a modern Desktop GUI and a robust Headless CLI mode.

✨ Key Features
Dual Interface: Use the intuitive Tkinter Desktop GUI or the Headless CLI for automated workflows.

High Performance: Multi-threaded processing (SIRENEWorker) ensures fast data fetching without freezing the interface.

Smart Resilience: 

- Auto-Checkpointing: If the process is interrupted, it saves progress automatically. Resume exactly where you left off.

- Intelligent Retries: Built-in handling for rate limits (HTTP 429) and network timeouts.

Advanced Data Cleaning: Automatically maps obscure INSEE codes to human-readable labels (e.g., NAF codes, legal categories, and employee size brackets).

Global Caching: Maintains a local cache (.sirene_cache) to avoid redundant API calls for previously enriched SIRETs.

Data Visualization: Real-time stats and charts showing activity sectors (NAF) and regional distribution of your data.

🛠️ Installation
1. Prerequisites
Python 3.8+

INSEE API Key: Obtain your free key from the Insee Developer Portal.

2. Setup
Clone the repository and install the required libraries:

Bash
git clone https://github.com/YOUR_USERNAME/sirene-enricher.git
cd sirene-enricher
pip install requests pandas openpyxl Pillow matplotlib
🚀 How to Use
Desktop Version (GUI)
Simply run the script to launch the interface:

Bash
python main.py
Paste your INSEE API Key.

Select your Excel File (ensure it contains a column named SIRET).

Click Start Enrichment.

Command Line Version (Headless)
For server environments or automation:

Bash
python main.py --headless --file data.xlsx --key YOUR_API_KEY --output enriched_results
Additional CLI Arguments:

--delay: Seconds between requests (default: 2.0).

--batch-size: Split output into smaller files (e.g., 50 rows per file).

--col: Change the target column name (default: "SIRET").

📊 Data Enriched
The tool adds the following information to your Excel file:

Denomination: Company name.

Legal Status: Human-readable company type (SAS, SARL, etc.).

Workforce: Employee range (e.g., "50 to 99 employees").

Activity: NAF code and full description.

Full Address: Number, street, city, postal code, and region.

IDCC: Collective agreement identifier (optional).

📁 Project Structure
main.py: The core application engine.

setup.iss: Configuration for the Inno Setup installer.

.sirene_cache/: Stores your session checkpoints and global cache (created on first run).

README.md: Project documentation.

⚙️ Configuration for Developers
If you are modifying the code, note the following thread-safe implementations:

_stats_lock: Ensures accurate counting across multiple worker threads.

_checkpoint_lock: Prevents file corruption during automatic progress saving.

📝 License
Distributed under the MIT License. See LICENSE for more information.

👨‍💻 Author
Saad Janina Automation & Digital Transformation
