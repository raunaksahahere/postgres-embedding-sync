# PyInstaller spec for the embedder bundled into the embedsync installers.
# Build (from embedder/):  uv run --group package pyinstaller ../packaging/embedder/embedder.spec
# Output: dist/embedsync-embedder/ (one directory; started by `embedsync` itself).
from PyInstaller.utils.hooks import collect_all, collect_submodules

datas, binaries, hiddenimports = [], [], []
for pkg in ("sentence_transformers", "tokenizers", "safetensors", "huggingface_hub"):
    d, b, h = collect_all(pkg)
    datas += d
    binaries += b
    hiddenimports += h
hiddenimports += collect_submodules("uvicorn")
hiddenimports += ["embedder", "embedder.app"]

a = Analysis(
    ["entry.py"],
    pathex=["../../embedder/src"],
    binaries=binaries,
    datas=datas,
    hiddenimports=hiddenimports,
    excludes=["tkinter", "matplotlib", "IPython", "tensorboard", "torchvision", "torchaudio",
              "pytest", "PIL.ImageQt", "PyQt5", "PyQt6", "PySide6"],
    noarchive=False,
)
# Drop parts of torch that inference never loads (test suites, headers, build
# tools). torch/bin keeps only torch_shm_manager, which `import torch` requires.
import os


def _keep(dest):
    d = dest.replace("\\", "/")
    if d.startswith(("torch/test/", "torch/include/", "torch/share/")):
        return False
    if d.startswith("torch/bin/"):
        return os.path.basename(d).startswith("torch_shm_manager")
    return True


a.datas = [d for d in a.datas if _keep(d[0])]
a.binaries = [b for b in a.binaries if _keep(b[0])]

pyz = PYZ(a.pure)
exe = EXE(pyz, a.scripts, [], exclude_binaries=True, name="embedsync-embedder",
          console=True, upx=False, strip=False)
coll = COLLECT(exe, a.binaries, a.datas, name="embedsync-embedder", upx=False, strip=False)
