"""
Cria todas as tabelas no banco de dados.
Rodar: python scripts/init_db.py
"""

import sys
from pathlib import Path

# Adicionar raiz ao sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv

load_dotenv()

from sqlalchemy import inspect

from src.db.database import Base, engine
import src.db.models  # noqa: F401 — registra os models no Base.metadata


def main():
    print("=" * 50)
    print("  SOL - Criação de tabelas")
    print("=" * 50)
    print()

    print("Criando tabelas...")
    Base.metadata.create_all(bind=engine)

    inspector = inspect(engine)
    tabelas = inspector.get_table_names()

    print(f"Tabelas no banco ({len(tabelas)}):")
    for t in sorted(tabelas):
        print(f"  - {t}")

    print()
    print("Concluído.")


if __name__ == "__main__":
    main()
