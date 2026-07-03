"""
Validate corretude of Workload A vs Workload B results
"""

import numpy as np
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def validate_distance_matrices(
    matrix_a: np.ndarray,
    matrix_b: np.ndarray,
    tolerance: float = 1e-4
) -> bool:
    """
    Valida que duas matrizes de distância são equivalentes.
    
    Args:
        matrix_a: Matriz de Workload-A
        matrix_b: Matriz de Workload-B
        tolerance: Tolerância para comparação (valores podem diferir por overhead numérico)
        
    Returns:
        True se as matrizes são equivalentes, False caso contrário
    """
    
    if matrix_a.shape != matrix_b.shape:
        logger.error(f"Shapes diferentes: {matrix_a.shape} vs {matrix_b.shape}")
        return False
    
    # Comparar valores finitos
    both_finite = np.isfinite(matrix_a) & np.isfinite(matrix_b)
    both_infinite = np.isinf(matrix_a) & np.isinf(matrix_b)
    
    if not np.all(both_finite | both_infinite):
        logger.warning("Finitude diferente em algumas posições")
        return False
    
    # Comparar distâncias finitas com tolerância
    if np.sum(both_finite) > 0:
        relative_error = np.abs(matrix_a[both_finite] - matrix_b[both_finite]) / (np.abs(matrix_b[both_finite]) + 1e-6)
        max_error = np.max(relative_error)
        
        if max_error > tolerance:
            logger.error(f"Erro relativo máximo: {max_error} (tolerância: {tolerance})")
            return False
        else:
            logger.info(f"Erro relativo máximo aceitável: {max_error}")
    
    logger.info("✓ Matrizes validadas como equivalentes")
    return True


def main():
    """Valida resultados de um experimento"""
    import sys
    
    if len(sys.argv) != 3:
        print("Uso: validate_results.py <resultado_a.npz> <resultado_b.npz>")
        sys.exit(1)
    
    path_a = Path(sys.argv[1])
    path_b = Path(sys.argv[2])
    
    logger.info(f"Carregando {path_a}")
    data_a = np.load(path_a)
    matrix_a = data_a[data_a.files[0]]  # Primeira matriz armazenada
    
    logger.info(f"Carregando {path_b}")
    data_b = np.load(path_b)
    matrix_b = data_b[data_b.files[0]]
    
    if validate_distance_matrices(matrix_a, matrix_b):
        logger.info("Validação OK")
        sys.exit(0)
    else:
        logger.error("Validação FALHOU")
        sys.exit(1)


if __name__ == '__main__':
    main()
