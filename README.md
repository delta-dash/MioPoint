# Miopoint

### Your smart, self-hosted media server.

Welcome to Miopoint—the open-source media server built around you. Miopoint is designed to be the central "point" for your entire media universe. The "Mio" signifies "my," because it starts with your personal library. Using a powerful AI engine to automatically scan your content, adding intelligent tags, extracting text with OCR, and detecting scenes, making everything instantly searchable.

But it's more than a personal tool. Miopoint is a meeting point for community. Share your discoveries, collaborate with others, and turn a simple collection of files into a living, connected library.

---

## ✨ Key Features

-   🚀 **High-Performance Backend**: Built on FastAPI for a robust, modern, and fast API.
-   📂 **Automatic Library Scanning**: A background service automatically discovers and indexes new photos and videos from your specified directories.
-   🧠 **Intelligent Content Analysis**:
    -   **AI Tagging**: Uses CLIP and other models to understand and tag the content of your media.
    -   **Text Recognition (OCR)**: Extracts and indexes text from images, making signs, documents, and screenshots searchable.
    -   **Scene Detection**: Automatically identifies and separates distinct scenes in your videos.
-   👥 **Multi-User & Guest Access**: Secure your library with a complete authentication system, including support for different user roles, permissions, and temporary guest sessions.
-   💬 **Real-time Interaction**: WebSocket support enables features like live notifications and messaging.
-   🖥️ **Effortless Setup**: A simple, one-time GUI (built with Kivy) appears on the first run to guide you through creating your admin account.

## 🗺️ Roadmap

Of course! Here is your list with a suitable emoji for every item.

### 🔥 High Priority
-   🚀 [ ] **First Major Release**: Package and release version 1.0.
-   🧠 [ ] **Expanded AI Models**: Integrate more models for advanced content description, style analysis, and more.
-   📥 [ ] **URL Ingestion**: Download and import media directly from web URLs.
-   🎶 [ ] **Audio Reverse Search**: Identify media based on an audio clip (Shazam-like).
-   ✂️ [ ] **Basic Editing Tools**: Implement server-side tools for cropping, trimming, and simple file edits.

### 🐢 Lower Priority
-   🎬 [ ] **Optimized Scene Tagging**: Improve the speed and accuracy of tagging individual video scenes.
-   📎 [ ] **Chat Attachments**: Allow users to attach media files to chat messages.
-   🎤 [ ] **Lyric Extraction**: Automatically fetch and index lyrics for music files.
-   🔌 [ ] **Plugin System**: Develop an architecture to allow for community-built extensions.
-   👥 [ ] **Guest System**: Fix the guest system or remove it completetly.
-   📺 [ ] **Live Streaming**: Add support for streaming media directly from the server.
-   🌐 [ ] **Peer Discovery**: Implement trackers or other methods for users to find and connect with other instances.
-   🤝 [ ] **Inter-User Sharing**: Enable seamless content sharing between registered users.
-   📈 [ ] **Upload Progress**: Provide real-time feedback for file uploads.
-   🔗 [ ] **Node-Based Workflows**: Create a system for users to build custom media processing pipelines.

## 🛠️ Tech Stack

-   **Backend**: FastAPI, Uvicorn
-   **Database**: SQLite (via `aiosqlite`)
-   **Initial Setup GUI**: Kivy, KivyMD
-   **AI / Machine Learning**: `transformers`, `onnxruntime-gpu`, OpenAI CLIP, `easyocr`
-   **Media Processing**: `opencv-python`, `scenedetect`, `ffmpeg-python`, `Pillow`
-   **Authentication**: JWT, Passlib, Bcrypt

## 🚀 Getting Started

Follow these steps to get your own Miopoint instance up and running.

### 1. Prerequisites

-   Python 3.12+
-   [FFmpeg](https://ffmpeg.org/download.html): Must be installed and available in your system's `PATH`.

### 2. Installation

First, clone the repository and navigate into the project directory.
```bash
git clone <repository-url>
cd Media-Server
```

Next, it is highly recommended to create and activate a virtual environment.
```bash
# Create the environment
python -m venv venv

# Activate it (Linux/macOS)
source venv/bin/activate

# Activate it (Windows)
venv\Scripts\activate
```

Finally, install the required Python packages.
```bash
pip install -r requirements.txt
```

### 3. First-Time Run

Miopoint makes the initial setup process simple.

1.  **Launch the application:**
    ```bash
    python main.py
    ```

2.  **Create Your Admin Account:**
    -   On the first run, a GUI window will automatically appear, prompting you to create the initial admin user.
    -   Enter your desired username and password and click submit.
    -   The window will close upon successful creation.

3.  **Server is Live!**
    -   Once an admin user exists, the FastAPI server will start automatically.
    -   Your Miopoint instance is now running and available at `http://0.0.0.0:8000`.

## 🔌 Using the API

Miopoint is powered by a FastAPI backend, which includes beautiful, interactive API documentation out-of-the-box. Once the server is running, you can explore the API endpoints:

-   **Swagger UI**: `http://127.0.0.1:8000/docs`
-   **ReDoc**: `http://127.0.0.1:8000/redoc`


🔗 **If you want a Webui:** [delta-dash/MioPoint-frontend](https://github.com/delta-dash/MioPoint-frontend)

## 🤝 Contributing

We welcome contributions of all kinds! If you'd like to help improve Miopoint, please follow these steps:

1.  Fork the repository.
2.  Create your feature branch (`git checkout -b feature/AmazingFeature`).
3.  Commit your changes (`git commit -m 'Add some AmazingFeature'`).
4.  Push to the branch (`git push origin feature/AmazingFeature`).
5.  Open a Pull Request.

---

