## **File System Interaction**
- `pwd` – Print the current working directory.  
- `ls` – List files in a directory (`ls -la` for detailed list).  
- `cd <directory>` – Change directory (`cd ..` to move up).  
- `mkdir <directory>` – Create a new directory.  
- `touch <file>` – Create an empty file.  
- `cp <source> <destination>` – Copy files or directories (`cp -r` for directories).  
- `mv <source> <destination>` – Move/rename files or directories.  
- `rm <file>` – Remove a file (`rm -r <directory>` for directories).  
- `find <path> -name "<filename>"` – Search for a file by name.  
- `locate <filename>` – Quickly find files (requires `updatedb`).  
- `cat <file>` – Display file contents.  
- `less <file>` – View file contents interactively.  
- `head -n <N> <file>` – Show the first N lines of a file.  
- `tail -n <N> <file>` – Show the last N lines of a file.  
- `chmod +x <file>` – Make a file executable.  

---

## **Downloading Files**
- `wget <URL>` – Download a file from a URL.  
- `curl -O <URL>` – Download a file from a URL (alternative to `wget`).  
- Download files from GDrive using `gdown`: (Pip-GDown)[https://pypi.org/project/gdown/]

---

## **Git Basics**
- `git clone <repo-url>` – Clone a repository.  
- `git status` – Show the status of changes.  
- `git add <file>` – Stage a file for commit (`git add .` for all changes).  
- `git commit -m "Commit message"` – Commit staged changes.  
- `git pull` – Get the latest changes from a remote repository.  
- `git push` – Push commits to a remote repository.  
- `git log --oneline` – View commit history.  
- `git checkout <branch>` – Switch branches.  
- `git merge <branch>` – Merge another branch into the current one.  

---

## **Running Scripts**
- `./script.sh` – Run a shell script (if executable).  
- `python3 script.py` – Run a Python script.  
- `chmod +x script.sh` – Make a script executable.  

---

## **Docker Basics (all interaction with systems and big-data tools MUST be via Docker)**
- `docker ps` – List running containers.  
- `docker ps -a` – List all containers (including stopped ones).  
- `docker images` – Show available Docker images.  
- `docker pull <image>` – Download a Docker image from Docker Hub.  
- `docker run -d --name <container_name> <image>` – Run a container in the background.  
- `docker run -it <image> bash` – Run a container interactively with a Bash shell.  
- `docker exec -it <container_name> bash` – Open a Bash shell in a running container.  
- `docker stop <container_name>` – Stop a running container.  
- `docker start <container_name>` – Start a stopped container.  
- `docker restart <container_name>` – Restart a container.  
- `docker rm <container_name>` – Remove a stopped container.  
- `docker logs <container_name>` – Show logs of a running container.  
