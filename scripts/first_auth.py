"""
Autorização inicial OAuth do Bling v3.
Rodar 1x localmente: python scripts/first_auth.py
"""

import sys
import threading
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

# Adicionar raiz ao sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv

load_dotenv()

from src.config import get_settings
from src.db.database import Base, SessionLocal, engine
from src.auth.oauth import exchange_authorization_code

# ── Estado global ────────────────────────────────────────────────────────────

auth_code: str | None = None
callback_received = threading.Event()

# ── Callback handler ─────────────────────────────────────────────────────────


class CallbackHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        global auth_code
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)

        code = params.get("code", [None])[0]
        if code:
            auth_code = code
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(
                b"<html><body><h1>&#10004; Autorizado com sucesso!</h1>"
                b"<p>Pode fechar esta janela.</p></body></html>"
            )
            callback_received.set()
        else:
            self.send_response(400)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(
                b"<html><body><h1>&#10060; Erro</h1>"
                b"<p>Parametro 'code' nao recebido.</p></body></html>"
            )

    def log_message(self, format, *args):
        pass  # Silenciar logs do HTTPServer


# ── Main ─────────────────────────────────────────────────────────────────────


def main():
    settings = get_settings()
    db = SessionLocal()

    try:
        # a) Criar tabelas
        print("=" * 60)
        print("  SOL - Primeira autorização OAuth Bling v3")
        print("=" * 60)
        print()
        print("[1/5] Criando tabelas no banco...")
        Base.metadata.create_all(bind=engine)
        print("       Tabelas criadas.")

        # b) Iniciar servidor local
        print("[2/5] Iniciando servidor local na porta 8000...")
        server = HTTPServer(("127.0.0.1", 8000), CallbackHandler)
        server_thread = threading.Thread(target=server.serve_forever, daemon=True)
        server_thread.start()
        print("       Servidor rodando em http://127.0.0.1:8000")

        # c) Montar URL de autorização
        auth_url = (
            "https://api.bling.com.br/Api/v3/oauth/authorize"
            f"?response_type=code"
            f"&client_id={settings.BLING_CLIENT_ID}"
            f"&state=first_auth"
        )

        # d) Abrir navegador
        print("[3/5] Abrindo navegador para autorização...")
        print()
        print(f"       URL: {auth_url}")
        print()
        webbrowser.open(auth_url)

        # e) Aguardar callback
        print("[4/5] Aguardando callback (timeout: 5 minutos)...")
        if not callback_received.wait(timeout=300):
            print()
            print("ERRO: Timeout! Nenhum callback recebido em 5 minutos.")
            print("       Tente novamente.")
            sys.exit(1)

        # f) Trocar code por tokens
        print(f"       Code recebido: {auth_code[:20]}...")
        print("[5/5] Trocando code por tokens...")
        data = exchange_authorization_code(db, auth_code)

        # g) Sucesso
        expires_in = data.get("expires_in", "?")
        print()
        print("=" * 60)
        print("  SUCESSO! Tokens salvos no banco de dados.")
        print(f"  Access token expira em: {expires_in} segundos")
        print(f"  Refresh token: salvo (uso unico, 30 dias)")
        print("=" * 60)

        # h) Shutdown
        server.shutdown()

    except Exception as e:
        print()
        print(f"ERRO: {e}")
        sys.exit(1)

    finally:
        db.close()


if __name__ == "__main__":
    main()
