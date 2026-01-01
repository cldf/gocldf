"""
"""
import time
import shutil
import os.path
import tempfile
import pathlib
import subprocess

from clldutils.misc import format_size

PROJECTS = pathlib.Path(os.path.expanduser('~')) / "projects"
DATASETS = [
    "grambank/grambank-cldf/cldf/StructureDataset-metadata.json",
    "glottolog/glottolog-cldf/cldf/cldf-metadata.json",
    "cldf-datasets/lgr/cldf/Generic-metadata.json",
    "cldf-datasets/languageatlasofthepacificarea/cldf/Generic-metadata.json",
    "cldf-datasets/doreco/cldf/Generic-metadata.json",
]
# FIXME: add diagnostic SQL queries and results per dataset!


def run():
    wd = pathlib.Path(__file__).parent.parent
    subprocess.check_call("go build .".split(), cwd=wd)

    with tempfile.TemporaryDirectory() as temp:
        temp = pathlib.Path(temp)
        bin = temp / "gocldf"
        shutil.copy(wd / "gocldf", bin)
        assert bin.exists()
        for ds in DATASETS:
            out = temp / "db.sqlite"
            s = time.time()
            print("{} ...".format(ds))
            res = subprocess.check_output([str(bin), "createdb", str(PROJECTS / ds), str(out), "-f"])
            assert "Loaded" in res.decode("utf8")
            print("... {:.1f}s\t{}".format(time.time()-s, format_size(out.stat().st_size)))

if __name__ == "__main__":
    run()
