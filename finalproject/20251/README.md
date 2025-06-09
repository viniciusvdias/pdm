# Instructions for submitting your Final Project (2025/1)

‼️ **Make sure you follow all the following instructions. Submissions not following these steps may not be graded.** ‼️

Follow these steps to submit your project by creating a pull request in the `viniciusvdias/pdm` repository under the `finalproject/20251/` directory.

## 1. Fork the repository

1. Go to the [pdm repository](https://github.com/viniciusvdias/pdm).
2. Click the **Fork** button at the top-right of the page to create a copy under your GitHub account.

## 2. Clone your fork

Open your terminal and run:

```bash
git clone https://github.com/YOUR-USERNAME/pdm.git
cd pdm
```

Replace `YOUR-USERNAME` with your GitHub username.

## 3. Create a new branch

Create a branch for your work:

```bash
git checkout -b finalproject-20251-GROUPID
```

Replace `GROUPID` with your group identifier (for example: G1, G2, G3, etc).

## 4. Start from the template project

A template project with instructions that must be followed is available under `finalproject/20251/gX`.

- Copy the template directory to your group-specific folder. For example, if your group is G1:

  ```bash
  cp -r finalproject/20251/gX finalproject/20251/g1
  ```

- Replace `g1` with your actual group folder name (for other groups, use `g2`, `g3`, etc).
- Read and follow all instructions in the template project before adding or modifying any files.

## 5. Add your project files

Work only inside your group’s folder under `finalproject/20251/<GROUPID>`.  
Do not place files directly in `finalproject/20251/` or in other groups’ folders.

## 6. Stage and commit your changes

Add and commit only your group’s folder to make sure only your files are included:

```bash
git add finalproject/20251/GROUPID
git commit -m "PDM - final project for 2025/1 - GROUPID"
```

Replace `GROUPID` with your group identifier.

## 7. Push changes to your branch

Push your branch to your fork:

```bash
git push origin finalproject-20251-GROUPID
```

You can push changes to your branch multiple times as you make progress. Only after you have completed your work, create a pull request.  
The pull request you create will be used for grading your final project.

## 8. Create a pull request (only one submission per group)

1. Go to your forked repository on GitHub.
2. Click the “Compare & pull request” button.
3. Make sure the pull request is targeted at the `viniciusvdias/pdm` repository, **main** branch.
4. Add a description and submit your pull request.

## Example for group G1

- Project directory: `finalproject/20251/g1`
- Store all your project files inside this folder. Do not place files in `finalproject/20251/` or in any other group's folder.
- Copy the template from `finalproject/20251/gX` to `finalproject/20251/g1` and follow the instructions in the template.
