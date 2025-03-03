# Big-Data: Massive Data Processing

## Getting started

1. This course's tools assume a Linux/UNIX system. In case you do not have
access yet, [install it in your computer natively](https://ubuntu.com/tutorials/install-ubuntu-desktop#1-overview), or install on Windows via [Windows Linux Subsystem (WSL)](https://learn.microsoft.com/pt-br/windows/wsl/)
2. Clone this repository:

```
git clone https://github.com/viniciusvdias/pdm
```

3. Change directory to the repository and build necessary tools (this may take
   a while, but must be done once):

```
cd pdm
make
```

## Getting to know this repository

- `docs/`: Misc documentation and mini-tutorials, links, study references, etc.
- `hostdir/`: Mapped as a volume in docker containers for persisting of your work (notebooks, processed files, etc.)
- `jupytercli/`: Client for interacting with Docker containers via [Jupyter](https://jupyter.org/)
- `neo4j/`: Graph system used in this course
- `spark/`: General-purpose system used in this course
- `vmaccess/`: Used to access this course'S VMs
- (Assignment) `exercises/`: hands-on exercises, day-to-day individual assignments,
- (Assignment) `seminars/`: topics in Big-Data, group assignment
- (Assignment) `finalproject/`: final project, group assignment

## Additional course material

- [INTRO - Introduction](https://docs.google.com/presentation/d/18VEGdulAowiQJ9OXPXUxge2fQg38etU7NN3YSMVvmC0/edit?usp=sharing)
- [SYS - Fundamentals of Parallel/Distributed Systems](https://docs.google.com/presentation/d/11KmZmuRXqUfmWVTNKh2wqtjtwFwfAsf0RqWaTh6Gwnk/edit?usp=sharing)
- [DATA - Fundamentals of Big-Data Systems](https://docs.google.com/presentation/d/1etproR5qdOgRkG-aY1EKkErUmd1Nm7fMKGSjy3zD6Qk/edit?usp=sharing)
- [BATCH - Batch processing 1](https://docs.google.com/presentation/d/17iuE9aKG_NRJ1ui_YVQ5IV3yIyxG1OTvY-iQfOdJy6E/edit?usp=sharing)
- [BATCH - Batch processing 2](https://docs.google.com/presentation/d/1jv5srrBMUQhbORXOA9bhhfFG3goHJQiR0Anxrn9Hrnw/edit?usp=sharing)
- [STRUCT - Structured Data processing 1](https://docs.google.com/presentation/d/1oLYEyYSp-gUPbP5Kx2affD-SpdzPYrKdLGXH32ezSnQ/edit?usp=sharing)
- [STRUCT - Structured Data processing 2](https://docs.google.com/presentation/d/1LYTM0Nk91MLxUdZZca5VUkBLXjvlJ-70ZyhTSR6ix0A/edit?usp=sharing)
- [STREAM - Stream processing 1](https://docs.google.com/presentation/d/1Rl6a1lHvzS3kI8c1umg9pDwXj5NgbOcmOh_om3ZDnH0/edit?usp=sharing)
- [STREAM - Stream processing 2](https://docs.google.com/presentation/d/1aZlHeMqU8l7AmBQl58yJWqr2Nv-0aD6vIJ-r2tXfeSs/edit?usp=sharing)
- [GRAPH - Graph processing 1](https://docs.google.com/presentation/d/16q9uV-SLYzERZsdAxeJfKMhhgQLFyUkkl4xeapCBM0c/edit?usp=sharing)
- [GRAPH - Graph processing 2](https://docs.google.com/presentation/d/1BodgQ8EtKJd4UWqIONqz0OTeSqNSkdb7SQwTD6cdtCY/edit?usp=sharing)

