"""Allow the package to be invoked with ``python -m housing_elt``."""

from housing_elt.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
