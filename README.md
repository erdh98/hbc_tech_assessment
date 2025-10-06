# NYC 311 Requests

This project analyzes and predicts complaint calls in NYC 311 data to inform city resource allocation.

---

## Project Setup

### 1. Clone the repository + change directory
```bash
git clone https://github.com/erdh98/hbc_tech_assessment.git

cd hbc_tech_assessment
```
### 2. Install Poetry (if necessary)
```bash
pip install poetry
```

### 3. Create and activate the Poetry virtual environment
```bash
poetry install
```

### 4. Add the Poetry environment as a Jupyter kernel
```bash
poetry run python -m ipykernel install --user --name=hbc_tech_assessment --display-name "Python (hbc_tech_assessment)"
```

### 5. Start Jupyter Notebook
```bash
poetry run jupyter notebook
```

### 6. Change kernel in Jupyter 
Kernel → Change Kernel → Python (hbc_tech_assessment)

## Notebooks
1. Ensure both notebooks are connected to the kernel `Python (hbc_tech_assesment)`
2. Run ETL + EDA.ipynb <u><b>first</b></u>
3. Run Final report.ipynb


