import os
import subprocess
import urllib.parse
from http.server import BaseHTTPRequestHandler, HTTPServer

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENGINE_PATH = os.path.join(BASE_DIR, "engine")
FORWARD_IDX = os.path.join(BASE_DIR, "forward.idx")
INVERTED_IDX = os.path.join(BASE_DIR, "inverted.idx")


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        html = """
        <html><body>
        <h1>Поиск</h1>
        <form>
            <input name="q" size="50">
            <input type="submit" value="Искать">
        </form>
        <hr>
        """

        if "?" in self.path:
            query_part = self.path.split("?")[1]
            params = urllib.parse.parse_qs(query_part)
            query = params.get("q", [""])[0]

            if query:
                query = urllib.parse.unquote(query)
                print(f"Поисковый запрос: '{query}'")

                try:
                    if not os.path.exists(ENGINE_PATH):
                        html += "<p style='color:red'>Файл engine не найден</p>"
                    elif not os.path.exists(FORWARD_IDX):
                        html += "<p style='color:red'>Файл forward.idx не найден</p>"
                    elif not os.path.exists(INVERTED_IDX):
                        html += "<p style='color:red'>Файл inverted.idx не найден</p>"
                    else:
                        original_cwd = os.getcwd()
                        os.chdir(BASE_DIR)

                        result = subprocess.run(
                            ["./engine", query],
                            capture_output=True,
                            text=True,
                            timeout=5,
                            encoding="utf-8",
                        )

                        os.chdir(original_cwd)

                        html += f"<h3>Результаты для '{query}':</h3>"
                        html += f"<pre>{result.stdout}</pre>"
                        if result.returncode != 0:
                            html += f"<pre style='color:red'>{result.stderr}</pre>"

                except Exception as e:
                    html += f"<p style='color:red'>Ошибка: {e}</p>"

        html += "</body></html>"

        self.send_response(200)
        self.send_header("Content-type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(html.encode("utf-8"))


if __name__ == "__main__":
    print(f"Базовая директория: {BASE_DIR}")
    print(f"Путь к engine: {ENGINE_PATH}")
    print(f"Путь к forward.idx: {FORWARD_IDX}")
    print(f"Путь к inverted.idx: {INVERTED_IDX}")
    print("Сервер запущен: http://localhost:8000")

    server = HTTPServer(("0.0.0.0", 8000), Handler)
    server.serve_forever()
