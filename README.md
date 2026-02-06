# UPS Maintenance Sorter

I wrote this script to stop manually filtering that massive UPS Smartsheet export.

Instead of staring at a 5,000-row spreadsheet, this script takes the raw export and splits it into clean, separate CSV files for every technician/contact. It tells them exactly what is **Overdue**, due in **by_calendar_year**, etc., without them having to search for it.

### What it actually does
1. **Splits by Contact:** It looks at the "Contact" column and makes a folder for every person (e.g., \`out/01-28-2026/by_contact/john_doe\`).
2. **Separates Batteries vs. Units:** You get separate files for battery replacements and full unit replacements.
3. **The "Smart" Logic:** * If a battery is due, but the *entire unit* is also due within a year, it **hides the battery alert**.
   * *Why?* So we don't waste money putting a brand new battery into a unit we are about to trash/replace anyway.
4. **NOC Summary:** It generates a \`summary.txt\` that gives the NOC a high-level view of how many devices are overdue in MDFs vs IDFs.

---

### How to use it

1. **Install dependencies** (just pandas and openpyxl for Excel files):
   \`\`\`bash
   pip install pandas openpyxl
   \`\`\`

2. **Run the script:**
   Point it at your exported file (Excel or CSV works).
   \`\`\`bash
   python ups_run.py "UPS Tracker Export.xlsx"
   \`\`\`

3. **Check the output:**
   Go to the \`out/\` folder. You'll see a folder with today's date. Inside is the summary and the folders for each contact.

---

### File Structure
* \`ups_run.py\` - The main logic. Run this one.
* \`ups_utils.py\` - Helper functions (date parsing, folder creation, etc.).
* \`out/\` - Where the generated CSVs go (this is ignored by Git).

### Note on Columns
The script tries to be smart about column names (it handles "Unit Serial" vs "Serial #"), but it generally expects the standard Smartsheet headers:
* Contact
* UPS Location and Hostname
* IP Address / MAC
* Battery Type / Unit Model
* Next Battery Replacement Date
* Unit Replacement Date

If the script crashes saying "Missing required columns," check that someone didn't rename the headers in the Excel file to something weird.