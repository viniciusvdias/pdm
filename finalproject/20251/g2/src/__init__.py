"""
Análise de dados do Restaurante Universitário da UFLA
Projeto de Big Data - Grupo 2

Este módulo configura automaticamente o sistema de logging usando loguru
"""

import os
from loguru import logger

__version__ = "0.1.0"
__author__ = "Grupo 2"


# Configurar logging automaticamente quando o módulo é importado
def _configure_logging():
    """Configura o sistema de logging do projeto"""
    try:
        from .logging_config import setup_logging

        # Obter configurações do ambiente
        log_level = os.getenv("LOG_LEVEL", "INFO")
        log_to_file = os.getenv("LOG_TO_FILE", "true").lower() == "true"
        enable_colors = os.getenv("LOG_COLORS", "true").lower() == "true"

        # Configurar logging
        setup_logging(
            log_level=log_level, log_to_file=log_to_file, enable_colors=enable_colors
        )

        logger.info("Módulo RU-UFLA Analytics inicializado")
        logger.info("Versão: {}", __version__)
        logger.debug("Logging configurado com nível: {}", log_level)

    except Exception as e:
        # Fallback para configuração básica se algo der errado
        logger.error("Erro ao configurar logging avançado: {}", e)
        logger.warning("Usando configuração básica de logging")


# Configurar logging na importação
_configure_logging()
