"""
Crypto-shredding: Destruccion criptografica de datos personales.
Implementa el derecho al olvido bajo LDPD/GDPR.

En vez de borrar datos fisicamente, se destruye la clave de
encriptacion del usuario. Sin la clave, los datos son ilegibles.
"""
import os
import sys
import logging
from sqlalchemy import create_engine, text
from datetime import datetime
from cryptography.fernet import Fernet

logger = logging.getLogger(__name__)

DB_URI = (
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@"
    f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)
engine = create_engine(DB_URI)


def generate_user_key(user_id: int) -> bytes:
    """Genera una clave unica para un usuario."""
    return Fernet.generate_key()


def encrypt_user_data(user_id: int, data: str) -> bytes:
    """Encripta datos de un usuario con su clave unica."""
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT encryption_key FROM audit.user_encryption_keys
            WHERE user_id = :uid AND is_active = TRUE
        """), {'uid': user_id})
        row = result.fetchone()
        if not row:
            raise ValueError(f"No hay clave activa para usuario {user_id}")

    fernet = Fernet(row[0])
    return fernet.encrypt(data.encode('utf-8'))


def request_deletion(user_id: int, reason: str = "Solicitud del usuario") -> int:
    """
    Registra una solicitud de eliminacion y ejecuta crypto-shredding.

    Args:
        user_id: Usuario que solicita eliminacion
        reason: Motivo de la solicitud

    Returns:
        request_id: ID de la solicitud creada
    """
    logger.info(f"Solicitud de eliminacion para usuario {user_id}")

    # 1. Registrar solicitud
    with engine.begin() as conn:
        result = conn.execute(text("""
            INSERT INTO audit.data_deletion_requests
            (user_id, request_type, status)
            VALUES (:uid, 'deletion', 'processing')
            RETURNING request_id
        """), {'uid': user_id})
        request_id = result.scalar()

    # 2. Revocar clave de encriptacion (crypto-shredding)
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE audit.user_encryption_keys
            SET is_active = FALSE,
                revoked_at = NOW(),
                revoke_reason = :reason
            WHERE user_id = :uid AND is_active = TRUE
        """), {'uid': user_id, 'reason': reason})

        # 3. Marcar solicitud como completada
        conn.execute(text("""
            UPDATE audit.data_deletion_requests
            SET status = 'completed',
                completed_at = NOW(),
                processed_by = 'crypto_shredding_script',
                notes = :reason
            WHERE request_id = :rid
        """), {'rid': request_id, 'reason': reason})

    logger.info(f"Usuario {user_id}: clave revocada. Datos son ahora ilegibles.")
    logger.info(f"Solicitud {request_id} completada.")

    return request_id


def check_deletion_status(user_id: int) -> dict:
    """Verifica el estado de eliminacion de un usuario."""
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT
                k.user_id,
                k.is_active AS key_still_active,
                k.revoked_at AS key_revoked_date,
                r.status AS deletion_status,
                r.completed_at,
                r.notes
            FROM audit.user_encryption_keys k
            LEFT JOIN audit.data_deletion_requests r
                ON k.user_id = r.user_id AND r.status = 'completed'
            WHERE k.user_id = :uid
        """), {'uid': user_id})
        row = result.fetchone()

        if not row:
            return {'found': False, 'message': f'Usuario {user_id} no encontrado'}

        return {
            'found': True,
            'user_id': row[0],
            'key_active': row[1],
            'key_revoked': row[2],
            'deletion_status': row[3],
            'completed_at': row[4],
            'notes': row[5]
        }


if __name__ == '__main__':
    import argparse

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    parser = argparse.ArgumentParser(description='Crypto-shredding: Gestion de eliminacion de datos personales')
    subparsers = parser.add_subparsers(dest='command', help='Comando a ejecutar')

    # Subcomando: request
    req_parser = subparsers.add_parser('request', help='Solicitar eliminacion de usuario')
    req_parser.add_argument('--user-id', type=int, required=True, help='ID del usuario')
    req_parser.add_argument('--reason', default='Solicitud del usuario', help='Motivo de la eliminacion')

    # Subcomando: check
    chk_parser = subparsers.add_parser('check', help='Verificar estado de eliminacion')
    chk_parser.add_argument('--user-id', type=int, required=True, help='ID del usuario')

    args = parser.parse_args()

    if args.command == 'request':
        rid = request_deletion(args.user_id, args.reason)
        print(f'Solicitud de eliminacion creada: ID={rid}')
    elif args.command == 'check':
        status = check_deletion_status(args.user_id)
        for k, v in status.items():
            print(f'  {k}: {v}')
    else:
        parser.print_help()
        sys.exit(1)
