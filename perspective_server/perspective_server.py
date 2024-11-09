# perspective_server.py

import threading
from perspective import Server
from perspective.handlers.tornado import PerspectiveTornadoHandler
import tornado.web
import tornado.ioloop

# Sample data to be displayed in the Perspective viewer
data = [
    {"Symbol": "AAPL", "ROC": 4.5, "TimeDiff": 100},
    {"Symbol": "GOOGL", "ROC": 3.8, "TimeDiff": 150},
    {"Symbol": "AMZN", "ROC": 5.0, "TimeDiff": 200},
    {"Symbol": "MSFT", "ROC": 2.1, "TimeDiff": 250}
]

def start_perspective_server(port=8888):
    server = Server()
    client = server.new_local_client()
    table = client.table(data, name="stock_data")

    app = tornado.web.Application([
        # Removed "check_origin": False
        (r"/websocket", PerspectiveTornadoHandler, {"perspective_server": server})
    ])

    app.listen(port)
    print(f"Perspective Server running at ws://localhost:{port}/websocket")
    tornado.ioloop.IOLoop.current().start()

def run_server_in_thread():
    server_thread = threading.Thread(target=start_perspective_server, daemon=True)
    server_thread.start()

# Initialize the server when this module is imported
run_server_in_thread()
