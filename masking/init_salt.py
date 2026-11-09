from __future__ import annotations

import os
import secrets
import sys

from airflow.models import Variable
from airflow.utils.session import create_session

KEY = "masking_salt"


def main() -> None:
    explicit = os.environ.pop("MASKING_SALT", "")
    if explicit:
        Variable.set(KEY, explicit)
        print(f"{KEY}: записана из MASKING_SALT")
        return
    with create_session() as session:
        row = session.query(Variable).filter(Variable.key == KEY).one_or_none()
        stored = None if row is None else row.get_val()
        if row is not None and not stored:
            sys.exit(
                f"ОШИБКА: Variable {KEY} есть в базе Airflow, но не расшифровывается текущим FERNET_KEY. "
                "Новая соль не создаётся, иначе сменятся все псевдонимы. "
                "Верните в .env прежний FERNET_KEY или удалите данные (docker compose down -v)."
            )
    if stored is None:
        Variable.set(KEY, secrets.token_hex(32))
        print(f"{KEY}: создана новая соль")
    else:
        print(f"{KEY}: используется сохранённая соль")


if __name__ == "__main__":
    main()
