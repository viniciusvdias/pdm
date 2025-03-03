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
