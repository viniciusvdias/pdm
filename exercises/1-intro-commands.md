# [EX1-INTRO] Download and explore a CSV file

> Assignment submission format: a single markdown file `ex1-intro-sol.md`.
>
> Need help with Markdown? There is a quick guide [here](https://docs.github.com/pt/get-started/writing-on-github/getting-started-with-writing-and-formatting-on-github/basic-writing-and-formatting-syntax).

## Reproduce the steps below

Append to each step below, the output of your terminal. Include a brief
explanation of the results: `pt-br` or `en`.

1. Create a working directory.

```bash
mkdir cnpj_exercise
cd cnpj_exercise
```

2. Download and unzip the public data [CNPJ - Regime Tributário - Lucro Real](https://arquivos.receitafederal.gov.br/dados/cnpj/regime_tributario/Lucro%20Real.zip).

```bash
wget https://arquivos.receitafederal.gov.br/dados/cnpj/regime_tributario/Lucro%20Real.zip
mv "Lucro Real.zip" lucro_real.zip # spaces in name are a pain :(
unzip lucro_real.zip -d lucro_real
mv "lucro_real/Lucro Real.csv" lucro_real/lucro_real.csv # spaces in name are a pain :(
```

3. Check the extracted files:

```bash
ls -lh lucro_real
```

4. File Size. To find out the size of each file in the extracted folder:

```bash
du -h lucro_real/*
```

5. Get Absolute File Paths. To list the absolute paths of the files:

```bash
realpath lucro_real/*
```

6. Identify File Types. To check whether a file is **text, binary, CSV, etc.**:

```bash
file lucro_real/*
```

7. Count the Number of Lines in a File. To count the number of rows (lines) in a file:  

```bash
wc -l lucro_real/*
```

8. Count the Number of Columns (CSV file).

```bash
head -n 1 lucro_real/lucro_real.csv | awk -F ',' '{print NF}'
```

- `head -n 1` extracts the first row.  
- `awk -F ',' '{print NF}'` counts the number of fields (columns).  

9. Count Unique Values in the First Column. To count distinct values in the **first column**:  

```bash
cut -d ',' -f1 lucro_real/lucro_real.csv | sort | uniq | wc -l
```

- `cut -d ',' -f1` extracts the first column.  
- `sort | uniq` removes duplicates.  
- `wc -l` counts the unique values.  

10. Automate Everything with a Bash Script. Instead of running each command manually, let's create a **Bash script** to automate the process.

```bash
nano extract_info.sh
```

- Paste the following script inside the file:

```bash
#!/bin/bash

DATA_DIR=$1 # first parameter to the script (command line)

if [[ ! -d "$DATA_DIR" ]]; then
    echo "Error: Directory '$DATA_DIR' not found!"
    exit 1
fi

for file in "$DATA_DIR"/*; do
    if [[ -f "$file" ]]; then
        echo "File: $(basename "$file")"
        echo "   Size: $(du -h "$file" | cut -f1)"
        echo "   Absolute Path: $(realpath "$file")"
        echo "   Type: $(file "$file" | cut -d':' -f2)"
        echo "   Lines: $(wc -l < "$file")"
        first_line=$(head -n 1 "$file")
        num_columns=$(echo "$first_line" | awk -F ',' '{print NF}')
        echo "   Columns: $num_columns"
        unique_values=$(cut -d ',' -f1 "$file" | sort | uniq | wc -l)
        echo "   Unique Values in Column 1: $unique_values"
        echo "------------------------------------"
    fi
done
```

- Save and exit (`CTRL+X`, `Y`, `ENTER`).

- Make the script executable (yes, not all files in a file system are allowed to
be executed):

```bash
chmod +x extract_info.sh
```

- Run the script:

```bash
./extract_info.sh lucro_real/
```
