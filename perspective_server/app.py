import streamlit as st
import streamlit.components.v1 as components
import threading
import time
from perspective import Server
from perspective.handlers.tornado import PerspectiveTornadoHandler
import tornado.web
import tornado.ioloop

# ----------------------------
# 1. Initialize Perspective Server
# ----------------------------

@st.cache_resource
def start_perspective_server(port=8888):
    def run_server():
        server = Server()
        client = server.new_local_client()
        
        # Sample data for Perspective
        data = [
            {"Symbol": "AAPL", "ROC": 4.5, "TimeDiff": 100},
            {"Symbol": "GOOGL", "ROC": 3.8, "TimeDiff": 150},
            {"Symbol": "AMZN", "ROC": 5.0, "TimeDiff": 200},
            {"Symbol": "MSFT", "ROC": 2.1, "TimeDiff": 250}
        ]
        client.table(data, name="stock_data")
        
        app = tornado.web.Application([
            (r"/websocket", PerspectiveTornadoHandler, {"perspective_server": server})
        ])
        app.listen(port)
        print(f"Perspective Server running at ws://localhost:{port}/websocket")
        tornado.ioloop.IOLoop.current().start()

    server_thread = threading.Thread(target=run_server, daemon=True)
    server_thread.start()
    time.sleep(2)

start_perspective_server(port=8888)

# ----------------------------
# 2. Define HTML Content with Perspective Viewer
# ----------------------------

PERSPECTIVE_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Perspective Viewer Scatter Plot</title>
    <!-- Load Perspective ESM modules -->
    <script type="module" src="https://cdn.jsdelivr.net/npm/@finos/perspective/dist/cdn/perspective.js"></script>
    <script type="module" src="https://cdn.jsdelivr.net/npm/@finos/perspective-viewer/dist/cdn/perspective-viewer.js"></script>
    <script type="module" src="https://cdn.jsdelivr.net/npm/@finos/perspective-viewer-d3fc/dist/cdn/perspective-viewer-d3fc.js"></script>
    <link rel="stylesheet" crossorigin="anonymous" href="https://cdn.jsdelivr.net/npm/@finos/perspective-viewer/dist/css/pro.css" />
    <style>
        body {
            display: flex;
            justify-content: center;
            align-items: center;
            height: 100vh;
            margin: 0;
        }
        perspective-viewer {
            width: 80%;
            height: 80vh;
        }
    </style>
</head>
<body>
    <!-- Perspective viewer element -->
    <perspective-viewer id="viewer"></perspective-viewer>

    <script type="module">
        import perspective from "https://cdn.jsdelivr.net/npm/@finos/perspective/dist/cdn/perspective.js";

        async function loadChart() {
            // Get the Perspective viewer element
            const viewer = document.querySelector("perspective-viewer");
            if (!viewer) {
                console.error("Perspective Viewer not found");
                return;
            }

            try {
                console.log("Initializing Perspective Worker...");
                const worker = await perspective.worker();
                
                // Sample data for the chart
                const data = [
                    { Symbol: "AAPL", ROC: 4.5, TimeDiff: 100 },
                    { Symbol: "GOOGL", ROC: 3.8, TimeDiff: 150 },
                    { Symbol: "AMZN", ROC: 5.0, TimeDiff: 200 },
                    { Symbol: "MSFT", ROC: 2.1, TimeDiff: 250 }
                ];
                
                // Create a Perspective table
                const table = await worker.table(data);
                
                // Load the table into the viewer
                await viewer.load(table);

                // Configure the viewer attributes
                viewer.setAttribute("view", "scatter"); // Use scatter plot view
                viewer.setAttribute("columns", '["TimeDiff", "ROC", "Symbol"]');
                viewer.setAttribute("plugin", "d3_xy_scatter");
                viewer.setAttribute("theme", "material-dark");
                
                console.log("Viewer loaded successfully with scatter plot");
            } catch (error) {
                console.error("Error loading Perspective data:", error);
            }
        }

        // Load the chart once the DOM is fully loaded
        window.addEventListener("DOMContentLoaded", loadChart);
    </script>
</body>
</html>

"""

components.html(PERSPECTIVE_HTML, height=700, scrolling=True)
