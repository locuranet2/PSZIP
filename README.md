# PSZIP: ZIP/RAR to FTP Streamer (Pro Edition)

**PSZIP** is a high-performance Python application designed to stream files directly from ZIP or RAR archives to an FTP server without ever extracting them to your local disk. It is ideal for uploading large gaming assets or updates where disk space is limited or speed is prioritized.

![PSZIP Demo](https://placehold.co/800x450?text=PSZIP+Interface+Preview)

## 🚀 Key Features

-   **Zero-Extraction Transfers**: Content is streamed directly from compressed archives to the remote host using RAM as a buffer.
-   **Multi-Format Support**: Works seamlessly with `.zip` and `.rar` (requires 7-Zip or WinRAR tools).
-   **Dynamic Concurrency Control**: Adjust the number of simultaneous uploads in real-time during a transfer without stopping.
-   **Full Persistence**: Switching interface languages during an active transfer preserves logs, connection status, and progress.
-   **Advanced Error Recovery**: Granular retry logic for failed items (Retry All or Retry Selected).
-   **Multi-tab Management**: Real-time status for Queued, Successful, and Failed transfers.
-   **Multilingual (7+ Languages)**: Dynamic loading from JSON for English, Spanish, Portuguese, German, French, Italian, and Chinese.

## 🛠️ Requirements

-   **Python 3.10+**
-   **Dependencies**:
    -   `rarfile`: For RAR support (`pip install rarfile`)
    -   **External Tools**: Make sure you have [7-Zip](https://www.7-zip.org/) or [WinRAR](https://www.rarlab.com/) installed if you plan on processing `.rar` files.

## 📥 Installation

1.  Clone the repository:
    ```bash
    git clone https://github.com/USER/PSZIP.git
    cd PSZIP
    ```
2.  Install requirements:
    ```bash
    pip install rarfile
    ```
3.  Run the application:
    ```bash
    python zip_to_ftp.py
    ```

## 🌍 Localization

PSZIP detects and loads translation keys dynamically from `i18n_zip_ftp.json`. You can add new languages or modify existing ones without touching the source code.

## 👨‍💻 Author

Developed by **@locuranet2**

## ⚖️ License

This project is licensed under the MIT License - see the LICENSE file for details.
