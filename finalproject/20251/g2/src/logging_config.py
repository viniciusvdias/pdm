"""
Configuração centralizada do sistema de logging usando loguru
"""

import sys
import os
from pathlib import Path
from loguru import logger


def setup_logging(
    log_level: str = "INFO",
    log_to_file: bool = True,
    log_dir: str = "/app/misc/logs",
    enable_colors: bool = True,
    enable_rotation: bool = True,
):
    """
    Configura o sistema de logging do projeto

    Args:
        log_level: Nível de log (DEBUG, INFO, WARNING, ERROR)
        log_to_file: Se deve salvar logs em arquivo
        log_dir: Diretório para arquivos de log
        enable_colors: Se deve usar cores no terminal
        enable_rotation: Se deve rotacionar arquivos de log
    """

    # Remover handler padrão do stderr
    logger.remove()

    # Configurar formato base
    base_format = (
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{module}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
        "<level>{message}</level>"
    )

    # Formato colorido para console
    console_format = base_format
    if enable_colors:
        console_format = (
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{module}</cyan> | "
            "<level>{message}</level>"
        )

    # Adicionar handler para console
    logger.add(
        sys.stdout,
        format=console_format,
        level=log_level,
        colorize=enable_colors,
        backtrace=True,
        diagnose=True,
    )

    # Configurar logging para arquivo se solicitado
    if log_to_file:
        # Garantir que o diretório existe
        Path(log_dir).mkdir(parents=True, exist_ok=True)

        # Arquivo de log geral
        log_file = os.path.join(log_dir, "ru_ufla_analytics.log")

        if enable_rotation:
            logger.add(
                log_file,
                format=base_format,
                level="DEBUG",
                rotation="10 MB",
                retention="30 days",
                compression="zip",
                backtrace=True,
                diagnose=True,
                enqueue=True,  # Thread-safe
            )
        else:
            logger.add(
                log_file,
                format=base_format,
                level="DEBUG",
                backtrace=True,
                diagnose=True,
                enqueue=True,
            )

        # Arquivo específico para erros
        error_file = os.path.join(log_dir, "errors.log")
        logger.add(
            error_file,
            format=base_format,
            level="ERROR",
            rotation="5 MB",
            retention="60 days",
            compression="zip",
            backtrace=True,
            diagnose=True,
            enqueue=True,
        )

        # Arquivo para métricas de performance
        metrics_file = os.path.join(log_dir, "performance.log")
        logger.add(
            metrics_file,
            format="{time:YYYY-MM-DD HH:mm:ss} | {extra[module]} | {message}",
            level="INFO",
            filter=lambda record: "performance" in record["extra"],
            rotation="20 MB",
            retention="90 days",
            compression="zip",
            enqueue=True,
        )

    # Configurar interceptor para logging padrão do Python
    _setup_standard_logging_interception()

    logger.info("Sistema de logging configurado com sucesso")
    logger.info("Nível de log: {}", log_level)
    logger.info("Log para arquivo: {}", log_to_file)
    if log_to_file:
        logger.info("Diretório de logs: {}", log_dir)


def _setup_standard_logging_interception():
    """Intercepta logs do módulo logging padrão do Python"""
    import logging
    import inspect

    class InterceptHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            # Obter nível correspondente do Loguru
            try:
                level = logger.level(record.levelname).name
            except ValueError:
                level = record.levelno

            # Encontrar caller de onde originou a mensagem
            frame, depth = inspect.currentframe(), 0
            while frame:
                filename = frame.f_code.co_filename
                is_logging = filename == logging.__file__
                is_frozen = "importlib" in filename and "_bootstrap" in filename
                if depth > 0 and not (is_logging or is_frozen):
                    break
                frame = frame.f_back
                depth += 1

            logger.opt(depth=depth, exception=record.exc_info).log(
                level, record.getMessage()
            )

    # Configurar interceptação
    logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)


def get_performance_logger():
    """Retorna um logger específico para métricas de performance"""
    return logger.bind(module="performance", performance=True)


def get_module_logger(module_name: str):
    """Retorna um logger para um módulo específico"""
    return logger.bind(module=module_name)


# Configurar automaticamente se executado como script
if __name__ == "__main__":
    setup_logging(log_level="DEBUG")

    perf_logger = get_performance_logger()
    perf_logger.info("Performance metric test")
