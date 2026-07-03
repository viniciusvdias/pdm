import subprocess
import sys
import time
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DOCKER_DIR = PROJECT_ROOT / "misc" / "docker"
POSTGRES_CONTAINER = "postgres_db"
POSTGRES_USER = "admin"
POSTGRES_DB = "incendios_db"
REFRESH_SECONDS = 2
ROW_LIMIT = 10


def run_command(command, cwd=None, check=True):
    return subprocess.run(
        command,
        cwd=cwd,
        check=check,
        text=True,
        capture_output=True,
    )


def ensure_docker_compose_up():
    print("Subindo a infraestrutura Docker...")
    result = run_command(["docker", "compose", "up", "-d"], cwd=DOCKER_DIR)
    if result.stdout.strip():
        print(result.stdout.strip())
    if result.stderr.strip():
        print(result.stderr.strip())


def wait_for_postgres(timeout_seconds=60):
    print("Aguardando o PostgreSQL ficar disponivel...")
    deadline = time.time() + timeout_seconds

    while time.time() < deadline:
        result = subprocess.run(
            [
                "docker",
                "exec",
                POSTGRES_CONTAINER,
                "pg_isready",
                "-U",
                POSTGRES_USER,
                "-d",
                POSTGRES_DB,
            ],
            text=True,
            capture_output=True,
        )
        if result.returncode == 0:
            print("PostgreSQL pronto para consultas.")
            return
        time.sleep(2)

    raise RuntimeError("Timeout aguardando o PostgreSQL ficar disponivel.")


def fetch_table_output():
    query = (
        "SELECT sensor_id, timestamp, temperatura, umidade, co2, "
        "status_ia_borda, indice_risco "
        "FROM historico_sensores "
        "ORDER BY timestamp DESC "
        f"LIMIT {ROW_LIMIT};"
    )
    result = run_command(
        [
            "docker",
            "exec",
            "-i",
            POSTGRES_CONTAINER,
            "psql",
            "-U",
            POSTGRES_USER,
            "-d",
            POSTGRES_DB,
            "-P",
            "pager=off",
            "-c",
            query,
        ],
        check=False,
    )
    return result.returncode, result.stdout.strip(), result.stderr.strip()


def clear_screen():
    subprocess.run(["clear"], check=False)


def monitor_loop():
    print("Monitorando os dados gravados no PostgreSQL. Pressione Ctrl+C para sair.")
    time.sleep(1)

    while True:
        clear_screen()
        print("=== MONITOR DE DADOS DO POSTGRESQL ===")
        print(f"Atualizacao a cada {REFRESH_SECONDS} segundos")
        print(f"Horario da consulta: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print()

        code, stdout, stderr = fetch_table_output()
        if code == 0:
            if stdout:
                print(stdout)
            else:
                print("Nenhum dado encontrado na tabela historico_sensores.")
        else:
            print("Falha ao consultar a tabela historico_sensores.")
            if stderr:
                print(stderr)
            if stdout:
                print(stdout)

        time.sleep(REFRESH_SECONDS)


def main():
    try:
        ensure_docker_compose_up()
        wait_for_postgres()
        monitor_loop()
    except KeyboardInterrupt:
        print("\nMonitoramento encerrado pelo usuario.")
    except FileNotFoundError as exc:
        print(f"Comando nao encontrado: {exc}")
        print("Verifique se o Docker esta instalado e disponivel no PATH.")
        sys.exit(1)
    except Exception as exc:
        print(f"Erro ao iniciar o monitoramento: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
