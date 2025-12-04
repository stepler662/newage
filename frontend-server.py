# frontend-server.py - простой сервер для фронтенда
import http.server
import socketserver
import webbrowser
import os

PORT = 3000

class CORSRequestHandler(http.server.SimpleHTTPRequestHandler):
    def end_headers(self):
        # Добавляем CORS headers ко всем ответам
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', '*')
        super().end_headers()
    
    def do_OPTIONS(self):
        # Обрабатываем preflight запросы
        self.send_response(200)
        self.end_headers()

print("=" * 50)
print("🚀 Запуск фронтенд-сервера...")
print(f"📡 Адрес: http://localhost:{PORT}")
print("=" * 50)

# Переходим в папку со скриптом
os.chdir(os.path.dirname(os.path.abspath(__file__)))

with socketserver.TCPServer(("", PORT), CORSRequestHandler) as httpd:
    print(f"✅ Сервер запущен! Открываю браузер...")
    
    # Автоматически открываем браузер
    webbrowser.open(f'http://localhost:{PORT}/index.html')
    
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n🛑 Сервер остановлен")