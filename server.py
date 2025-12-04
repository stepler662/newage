# server.py - простой сервер для фронтенда
import http.server
import socketserver
import webbrowser

PORT = 3000

class Handler(http.server.SimpleHTTPRequestHandler):
    def end_headers(self):
        # Добавляем CORS headers
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', '*')
        self.send_header('Access-Control-Allow-Headers', '*')
        super().end_headers()

print(f"🚀 Запуск фронтенд-сервера на http://localhost:{PORT}")
print("📁 Обслуживаю файлы из текущей папки")

with socketserver.TCPServer(("", PORT), Handler) as httpd:
    print(f"✅ Открой http://localhost:{PORT}/index.html в браузере")
    webbrowser.open(f'http://localhost:{PORT}/index.html')
    
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n🛑 Сервер остановлен")