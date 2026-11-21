"""Entry point for the frozen (PyInstaller) embedder shipped in the installers."""

import multiprocessing

from embedder.app import main

if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()
