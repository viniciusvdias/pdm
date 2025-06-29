# Template Project Structure (`gX`)

‼️ **Warning:** If you do not follow this folder and file structure, or if your project cannot be run out of the box using only Docker as described, your project may not be graded. ‼️

Your group’s project directory should follow the structure below, where `gX` must be replaced by your actual group ID (for example, `g1`, `g2`, etc). Each folder and file has a specific purpose. **Make sure you organize your project exactly as described here.**

```
gX/
├── bin/
├── src/
├── misc/
├── datasample/
├── presentation/
└── README.md
```

## 1. `bin/` — Binaries and scripts

- Put all executable files, scripts, or helper tools here.
- These are scripts or binaries used to run your project.
- You may provide scripts to run the project, but remember: **your project must work just by using Docker or Docker Compose commands, or by running scripts that use Docker.**

## 2. `src/` — Source code

- Place all code files for your project here.
- This includes your main program, modules, libraries, or any code you write as part of the solution.

## 3. `misc/` — Auxiliary files

- Add any extra files needed to run your project here (for example: configuration files, text files, sample input for scripts, etc).
- **Do not include datasets in this folder.**
- You must write in your report (`README.md`) clear instructions on how to get the data to run your project (for example, download links, scripts, cloud storage info, or instructions for generating synthetic data).

## 4. `datasample/` — Sample data

- Include a small sample of the data used in your project (at most 1MB; use compression if needed, and smaller is better).
- This sample is for quick testing and must not be the full dataset.
- If your data is generated, include instructions (in the README) on how to generate this sample using Docker.
- **Do not put large or full datasets here.**

## 5. `presentation/` — Slides

- This folder must contain a single PDF file with the slides for your group presentation.
- Name the file clearly (for example: `presentation.pdf`).

## 6. `README.md` — Documentation and results

- At the root of the group folder (`gX`), there must be a file named `README.md`.
- This file is your main documentation:
  - It explains how to install and run your project.
  - It describes your data, architecture, and main steps.
  - It shows your results and your discussion.
- **Important:** There will be a file called `README-report-template.md` in the template.  
  You must follow this template exactly for your `README.md` file, including all required sections and organization.

## Important: Docker and reproducibility

- **Your project MUST run out of the box with Docker.**
- The only thing that can be required to run your project is Docker itself. No other installation, configuration, or dependencies should be needed.
- Anyone must be able to run your complete project just by using Docker or by using the scripts you provide in `bin/` that use Docker/Docker Compose commands.
- You may assume your project will be reproduced on a Linux machine via terminal using Docker.
